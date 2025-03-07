import os
import ipywidgets as ipw
from IPython.display import display
import time
from .string_utils import *
from .repo_utils import *
from .aiida_and_ssh_utils import *
from datetime import datetime,timedelta
from aiida import load_profile
from aiida.orm import load_node
from aiida.orm import QueryBuilder, WorkChainNode, StructureData, Node

#ssh_config_data = config.get("ssh_config", "")
def check_for_updates(selected_grant=''):
    """Check if there is a new update available and pull changes if necessary."""
    
    # Ensure the repository exists
    if not os.path.exists(GIT_REPO_PATH):
        if not clone_repository():
            return "<b style='color:red;'>❌ Failed to clone the repository. Please check your configuration.</b>",{},{}
    
    local_commit = get_local_commit()
    remote_commit = get_latest_remote_commit()
    
    if not local_commit or not remote_commit:
        return "<b style='color:red;'>❌ Unable to check for updates.</b>",{},{}

    if local_commit != remote_commit:
        if not pull_latest_changes():
            return "<b style='color:red;'>❌ Failed to update the repository.</b>",{},{}
    
    status,msg,updates_needed,config = process_aiida_configuration(configuration_file, config_path,selected_grant)
    if not status:
        return msg,{},config
    if not updates_needed:
        return "<b style='color:green;'>✅ Your configuration is up to date.</b>",{},config
    else:
        return msg,updates_needed,config
    
def process_aiida_configuration(configuration_file, config_path,selected_grant):
    """
    Reads the YAML configuration file, renames the existing SSH config, 
    creates a new SSH config from the YAML file, and checks installed vs. missing AiiDA computers.
    
    :param configuration_file: Path to the YAML configuration file.
    :param config_path: Path to the SSH config directory.
    :return: Formatted string with the results.
    """
    updates_needed={}
    # Convert to Path objects
    configuration_file = Path(configuration_file)
    config_path = Path(config_path)
    result_msg = ""
    
    # Read the YAML configuration file
    with open(configuration_file, "r") as file:
        config = yaml.safe_load(file)
        
    if selected_grant == '':
        return False, "<b style='color:red;'>⬜  Please select a grant to enable checking.</b>",{},config
    
    # Check ssh_config
    config_ok,msg,config_hosts = check_ssh_config(config_path, config['computers'])
    result_msg +=msg
    if not config_ok:
        if 'not properly' in msg:
            updates_needed.setdefault('ssh_config', {})['rename'] =  True
        else:
            updates_needed.setdefault('ssh_config', {})['rename'] =  False
        updates_needed['ssh_config']['hosts'] = config_hosts
        
    # Get the list of active and not-active AiiDA computers
    status_computers,msg,active_computers,not_active_computers = aiida_computers()
    result_msg +=msg
    # Get the list of active and not-active AiiDA codes
    status_codes,msg,active_codes,not_active_codes = aiida_codes()
    result_msg +=msg
    if not (status_computers and status_codes):
        return False,result_msg + msg,{}
                           
        
    # Check if each defined computer exists in AiiDA and is up-to-date
    defined_computers = config.get("computers", {})

    # Checking for olld grants
    defined_grants = [grant for grants_list in config["grants"].values() for grant in grants_list]
    for computer in active_computers:
        if computer != 'localhost':
            its_grant = computer.split('_')[-1]
            if its_grant not in defined_grants:
                result_msg += f"⚠️ Computer '{computer}' is installed in AiiDA but its grant '{its_grant}' is not defined in the configuration file.<br>"
                updates_needed.setdefault('computers', {})[computer] = {'hide':True,'rename': False,'install':False}

    # Checking computers loop over grants
    for comp, comp_data in defined_computers.items():
        for grant in defined_grants:
            full_comp = f"{comp}_{grant}"
            if grant == selected_grant:
                install_this = True
            else:
                install_this = False
            if full_comp in active_computers:
                result_msg += f"⬜ Computer '{full_comp}' is already installed in AiiDA, checking for its configuration.<br>"
                is_up_to_date, msg = compare_computer_configuration(full_comp, comp_data)
                result_msg += msg
                if not is_up_to_date:  # Only add to updates_needed if not up-to-date
                    updates_needed.setdefault('computers', {})[full_comp] = {'hide':True,'rename': True,'install':install_this}

            elif full_comp in not_active_computers:
                result_msg += f"⬜ Computer '{full_comp}' is listed but NOT active in AiiDA.<br>"
                updates_needed.setdefault('computers', {})[full_comp] = {'hide':False,'rename': True,'install':install_this}

            else: #here distinguish between all grants and selected grant
                if grant in config['grants'][comp] and install_this:
                    result_msg += f"❌ Computer '{full_comp}' is completely missing from AiiDA.<br>"
                    updates_needed.setdefault('computers', {})[full_comp] = {'hide':False,'rename': False,'install':install_this}

    # Checking codes
    defined_codes = config.get("codes", {})

    # Check if each defined code exists in AiiDA and is up-to-date
    # in the yaml configuration a code definition also include the computer
    
    # To do. loop on active codes with grant that is old, --> hide
    for _, code_data in defined_codes.items(): 
        computer = code_data['computer']+f"_{selected_grant}"
        computer_up_to_date = computer not in updates_needed.get('computers', {})
        code_label = f"{code_data['label']}@{computer}"

        # Default: No update needed
        msg = f"✅ Code {code_label}@{computer} is already installed in AiiDA.<br>"

        if computer_up_to_date:  # Computer is up-to-date, check renaming needs
            code_pk = next((pk for codename, pk in active_codes if codename == f"{code_label}@{computer}"), None)
            if code_pk is not None:
                if not compare_code_configuration(code_label,code_data):
                    updates_needed.setdefault('codes', {})[code_label] = {'rename': code_pk,'install':True}
                    msg = f"⚠️ Code {code_label} is already installed in AiiDA but is old. Will be renamed and reinstalled.<br>"
            else:
                code_pk = next((pk for codename, pk in not_active_codes if codename == f"{code_label}@{computer}"), None)
                if code_pk is not None:
                    updates_needed.setdefault('codes', {})[code_label] = {'rename': code_pk,'install':True}
                    msg = f"⚠️ Code {code_label} is already installed (not active) in AiiDA but is old. Will be renamed and reinstalled.<br>"
                else:
                    updates_needed.setdefault('codes', {})[code_label] = {'rename': False,'install':True}
                    msg = f"⬜ Code {code_label} will be installed  {computer} is present.<br>"
        else: #I will install the computer thus the code does not have to be renamed
            updates_needed.setdefault('codes', {})[code_label] = {'rename': False,'install':True}
            msg = f"⬜ Code {code_label} will be installed after installation of {computer}. No need to rename.<br>"

        result_msg += msg   
    # To do: Check if cusntom app installations are needed
        

    return True,result_msg,updates_needed,config

def setup_computers(computers_to_setup,defined_computers,account=None):
    for computer in computers_to_setup:
        computer_name = computer.split('_')[0]
        _, _, grant = computer.partition('_') # gives '' if no _ is found
        print(f"🔄 Setting up computer '{computer_name}' with grant {grant} as {computer}")
        status = setup_aiida_computer(computer, defined_computers[computer_name],hide=computers_to_setup[computer].get('hide',False),
                             torelabel=computers_to_setup[computer].get('rename',False),
                             install=computers_to_setup[computer].get('install',False),
                             grant=grant
                             )
    return status
def setup_codes(codes_to_setup,config):
    defined_codes = config.get("codes", {})
    uenvs=[]
    for full_code in codes_to_setup:
        # pw-7.4:v2@daint.alps_s1267
        code = full_code.split('@')[0].split('-')[0] # pw
        code_data = defined_codes[code]
        computer = code_data['computer']
        hostname = config['computers'][computer]['setup']['hostname']
        prepend_text = code_data.get("prepend_text", "")
        match = re.search(r"#SBATCH --uenv=([\w\-/.:]+)", prepend_text)
        if match:
            uenv_value = match.group(1)  # Extract matched value
            print(f"⬜  Need uenv: {uenv_value} for '{full_code}'")
            if uenv_value not in uenvs:
                uenvs.append((hostname,uenv_value))
        else:
            print(f"✅ No uenv needed for '{full_code}'")
            
        status = setup_aiida_code(full_code, code_data,hide=codes_to_setup[full_code].get('hide',False),
                            relabel=codes_to_setup[full_code].get('rename',False),
                            install=codes_to_setup[full_code].get('install',False))
        
            

    return status,uenvs

# Manage uenvs

def manage_uenv_images(uenvs):
    """
    Ensure that required uenv images are available on a remote host.
    
    :param remote_host: The remote machine where commands will be executed.
    :param uenvs: A list of required uenv images (e.g., ['cp2k/2024.3:v2', 'qe/7.4:v2'])
    """

    # Step 1: Check if the uenv repo exists, if not, create it
    hosts = {uenv[0] for uenv in uenvs}
    for remotehost in hosts:
        print("🔍 Checking UENV repository status on {remotehost}")
        command = ["ssh", remotehost, "uenv", "repo", "status"]
        repo_status, command_ok = run_command(command)
        if not command_ok:
            print(f"❌ Failed to check UENV repo status on {remotehost}. Exiting.")
            return False
        
        if "not found" in repo_status.lower() or not repo_status or "no repository" in repo_status.lower() :
            print(f"⚠️ UENV repo not found. Creating repository...")
            command = ["ssh", remotehost, "uenv", "repo", "create"]
            command_out, command_ok = run_command(command)
            if not command_ok:
                print(f"❌ Failed to create UENV repo on {remotehost}. Exiting.")
                return False
        else:
            print("✅ UENV repo is available on {remotehost}.")

    # Step 2: Get the list of images available to the user
    available_images = {}
    for remotehost in hosts:
        print(f"🔍 Fetching available UENV images on {remotehost} for the user ")
        command = ["ssh", remotehost, "uenv", "image", "ls"]
        command_out, command_ok = run_command(command)
        print(command_out)
        print(extract_first_column(command_out))
        if not command_ok:
            print(f"❌ Failed to fetch UENV images on {remotehost}. Exiting.")
            return False
        available_images.setdefault(remotehost,{})['user'] = extract_first_column(command_out)

        # Step 3: Get the list of all images available on the system
        print("🔍 Fetching available UENV images (system-wide)")
        
        command = ["ssh", remotehost, "uenv", "image", "find"]
        command_out, command_ok = run_command(command)
        if not command_ok:
            print("❌ Failed to fetch system-wide UENV images. Exiting.")
            return False
        available_images.setdefault(remotehost,{})['host'] = extract_first_column(command_out)
        
        command = ["ssh", remotehost, "uenv", "image", "find", "service::"]
        command_out,command_ok = run_command(command)
        if not command_ok:
            print("❌ Failed to fetch service UENV images. Exiting.")
            return False    
        available_images.setdefault(remotehost,{})['service'] = extract_first_column(command_out)

    # Step 4: Check missing images and pull them if necessary
    for uenv in uenvs:
        env = uenv[1]
        remotehost = uenv[0]
        if env in available_images[remotehost]['user']:
            print(f"✅ Image '{env}' is already available for the user on {remotehost}.")
        elif env in available_images[remotehost]['host']:
            print(f"✅ Image '{env}' is available on the host {remotehost}. Pulling...")
            command = ["ssh", remotehost, "uenv", "image", "pull", env]
            command_out, command_ok = run_command(command)
        elif uenv in available_images[remotehost]['service']:
            print(f"✅ Image '{env}' is available in the service repo on {remotehost}. Pulling from service::...")
            command = ["ssh", remotehost, "uenv", "image", "pull", f"service::{env}"]
            command_out, command_ok = run_command(command)
        else:
            print(f"❌ Image '{env}' is not available anywhere on {remotehost}! Manual intervention needed.")
            return False

    print("✅ UENV management complete.")
    return True

# copying scripts and data directories to daint, may take a while

def copy_scripts(cscs_username,remotehost):# Define commands
    ssh_commands = [
        f"mkdir -p /users/{cscs_username}/src",
    ]

    scp_commands = [
        f"scp {alps_files}mps-wrapper.sh {remotehost}:/users/{cscs_username}/bin/",
        f"scp -r {alps_files}cp2k {remotehost}:/users/{cscs_username}/src/"
    ]

    # Execute SSH commands
    for cmd in ssh_commands:
        command_out,command_ok = run_command(cmd, ssh=True,remotehost=remotehost)
        if not command_ok:
            return

    # Execute SCP commands
    for cmd in scp_commands:
        command_out,command_ok = run_command(cmd)
        if not command_ok(cmd):
            return

# setup phoopy

def setup_phonopy(cscs_username):
    
    command = """
if [ ! -f Miniconda3-latest-Linux-aarch64.sh ] ; then
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh
fi"""
    command_out,comand_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not comand_ok:
        print("❌ Failed to download Miniconda3. Exiting, ask for help.")
        return False 
    
    command = f"""
if [ ! -d /users/{cscs_username}/miniconda3 ]; then
     bash Miniconda3-latest-Linux-aarch64.sh -b -p /users/{cscs_username}/miniconda3
fi"""
    command_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to install Miniconda3. Exiting, ask for help.")
        return False
    command_out,command_ok = run_command(f"scp {alps_files}bashrc_template {remotehost}:/users/{cscs_username}")
    if not command_ok:
        print("❌ Failed to copy bashrc_template. Exiting, ask for help.")
        return False
    command_out,command_ok = run_command("mv .bashrc .old_bashrc",ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to rename .bashrc. Exiting, ask for help.")
        return False
    command_out,command_ok = run_command("mv bashrc_template .bashrc",ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to rename bashrc_template. Exiting, ask for help.")
        return False
    command_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to copy bashrc_template. Exiting, ask for help.")
        return False
    command = "conda env list | grep -q '^phonopy ' && echo 'Environment exists' || conda create -n phonopy -c conda-forge phonopy seekpath"
    command_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to create phonopy environment. Exiting, ask for help.")
        return False
    _ = setup_codes(['phonopy.yml'])
    return True

def setup_critic2(cscs_username,qe_uenv):
    command = f"conda env list | grep -q '^py39 ' && echo 'Environment py39 exists' || conda create -n py39 -c conda-forge python={python_version} pymatgen scikit-image"
    command_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to create py39 environment. Exiting, ask for help.")
        return False
    _ = setup_codes(['python.yml'])
    
    command = f"""
if [ ! -d 'critic2' ]; then
git clone https://github.com/aoterodelaroza/critic2.git
fi"""
    command_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to clone critic2 repository. Exiting, ask for help.")
        return False
    command = f"""if [ ! -f f'/users/{cscs_username}/critic2/build/src/critic2' ]; then
cd critic2
mkdir build
cd build
uenv run {qe_uenv} cmake ..
uenv run {qe_uenv} make
fi"""
    command_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to build critic2. Exiting, ask for help.")
        return False
    command_out,command_ok = run_command(f"ls /users/{cscs_username}/critic2/build/src/critic2",ssh=True,remotehost=remotehost )
    if not command_ok:
        print("❌ critic2 not built. Exiting, ask for help.")
        return False
    _ = setup_codes(['critic2.yml'])
    return True


#### CHECK for old unfinished Workchains
def first_caller(node_pk, max_calls=5000):
    """
    Traces back to the first caller (root node) of a given node.
    
    :param node_pk: The PK of the node.
    :param max_calls: Maximum recursion depth to prevent infinite loops.
    :return: PK of the first caller.
    """
    num_calls = 0
    caller = node_pk
    while num_calls < max_calls:
        try:
            caller = load_node(caller).caller.pk
        except AttributeError:
            break  # No more parents, stop tracing
        num_calls += 1
    return caller

def get_structuredata_descendants(parent_pk):
    """
    Returns all StructureData nodes that are descendants of a given node.
    
    :param parent_pk: The PK of the parent node (e.g., WorkChain PK).
    :return: A list of StructureData node PKs.
    """
    qb = QueryBuilder()
    qb.append(Node, filters={'id': parent_pk}, tag='parent')
    qb.append(
        StructureData, 
        with_ancestors='parent',  # Search for descendants
        project=['id']  # Retrieve PKs only
    )
    return [entry[0] for entry in qb.all()]

def get_processes_with_structuredata_input(structure_pks):
    """
    Returns all CalcJob, WorkChain, and CalcFunction nodes that have 
    a given StructureData node as an input.
    
    :param structure_pks: List of StructureData PKs.
    :return: A list of process node PKs.
    """
    if not structure_pks:
        return []
    
    qb = QueryBuilder()
    qb.append(StructureData, filters={'id': {'in': structure_pks}}, tag='structure')
    qb.append(
        Node, 
        with_incoming='structure',  # Find nodes that receive the StructureData as input
        filters={'node_type': {'in': [
            'process.calculation.calcjob.CalcJobNode.',
            'process.workflow.workchain.WorkChainNode.',
            'process.calculation.function.CalcFunctionNode.'
        ]}},
        project=['id']  # Retrieve PKs only
    )
    return [entry[0] for entry in qb.all()]

def safe_to_delete(workchain_pk):
    """
    Determines if a WorkChainNode can be safely deleted.
    
    :param workchain_pk: The PK of the WorkChainNode.
    :return: True if it can be safely removed, False otherwise.
    """
    structure_pks = get_structuredata_descendants(workchain_pk)
    calcjobs = get_processes_with_structuredata_input(structure_pks)
    
    for job in calcjobs:
        if first_caller(job) != workchain_pk:
            return False
    return True

def get_old_unfinished_workchains():
    """
    Returns a formatted message with all WorkChainNodes that are older than 30 days and unfinished.
    
    :return: HTML formatted message with green (✅) and red (❌) indicators.
    """
    if not load_profile():
        load_profile("default")
    cutoff_date = datetime.now() - timedelta(days=30)
    
    qb = QueryBuilder()
    qb.append(
        WorkChainNode, 
        filters={
            'ctime': {'<': cutoff_date},  # Created more than 30 days ago
            'attributes.process_state': {'!in': ['finished', 'excepted', 'killed']}  # Not finished
        },
        project=['id']  # Retrieve PKs only
    )
    
    old_unfinished = [entry[0] for entry in qb.all()]
    if not old_unfinished:
        return "<style='color: green;'>✅ No old unfinished WorkChainNodes found.<br>"
    
    msg = "<style='color: darkorange;'>⚠️ Found old unfinished WorkChains<br>"
    msg += "<p>Ask for help if you are unsure about removing them.</p><ul>"
    
    for pk in old_unfinished:
        if safe_to_delete(pk):
            msg += f"<li style='color: green;'>✅ WorkChain <strong>PK {pk}</strong> can be safely removed.</li>"
        else:
            msg += f"<li style='color: red;'>❌ WorkChain <strong>PK {pk}</strong> cannot be safely removed.</li>"
    
    msg += "</ul>"
    return msg