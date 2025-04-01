import os
from copy import deepcopy
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

# Check repository of config files
def check_repository():
    """Check if the repository exists and pull the latest changes."""
    # Ensure the repository exists
    if not os.path.exists(GIT_REPO_PATH):
        if not clone_repository():
            return False,"<b style='color:red;'>❌ Failed to clone the repository. Please check your configuration.</b>"
    
    local_commit = get_local_commit()
    remote_commit = get_latest_remote_commit()
    
    if not local_commit or not remote_commit:
        return False,"<b style='color:red;'>❌ Unable to check for updates.</b>"

    if local_commit != remote_commit:
        if not pull_latest_changes():
            return False,"<b style='color:red;'>❌ Failed to update the repository.</b>"
    return True,"<b style='color:green;'>✅ Repository is up to date.</b>"  

def get_config(file_path='/home/jovyan/opt/aiidalab-alps-files/config.yml', config_widgets={}):
    """Get the configuration from the YAML file."""
    # Verifica che tutti i widget siano selezionati
    for key in config_widgets:
        if config_widgets[key].value == "select":
            return f"please select {key}", {}

    # Verifica lo stato del repository
    status_ok, msg = check_repository()
    if not status_ok:
        return msg, {}

    # Carica lo YAML
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)

    variables = data.get("variables", {})
    widgets = data.get("widgets", {})

    # Funzione per sostituire {key} in una stringa
    def replace_in_string(s, replacements):
        for key, value in replacements.items():
            s = s.replace(f"{{{key}}}", value)
        return s

    # Sostituzione ricorsiva in dizionari, liste e stringhe
    def recursive_replace(obj, replacements):
        if isinstance(obj, dict):
            return {k: recursive_replace(v, replacements) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [recursive_replace(item, replacements) for item in obj]
        elif isinstance(obj, str):
            return replace_in_string(obj, replacements)
        else:
            return obj

    # Prima passata: ottieni i valori dei widget
    widget_replacements = {
        key: config_widgets[key].value
        for key in widgets if key in config_widgets
    }

    # Sostituisci {widget} dentro alle variabili
    for key, value in variables.items():
        if isinstance(value, str):
            variables[key] = replace_in_string(value, widget_replacements)

    # Crea un dizionario con tutte le sostituzioni da applicare allo YAML intero
    all_replacements = {}
    all_replacements.update(widget_replacements)
    all_replacements.update({
        key: value for key, value in variables.items()
        if isinstance(value, str)
    })

    # Applica tutte le sostituzioni allo YAML
    data = recursive_replace(data, all_replacements)

    return '', data



def check_for_updates(config):
    """Checks teh config file."""    
    status,msg,updates_needed = process_aiida_configuration(config, config_path)
    if not status:
        return msg,{}
    if not updates_needed:
        return "<b style='color:green;'>✅ Your configuration is up to date.</b>",{}
    else:
        return msg,updates_needed
    
def process_aiida_configuration(config, config_path):
    """
    Reads the YAML configuration file, renames the existing SSH config, 
    creates a new SSH config from the YAML file, and checks installed vs. missing AiiDA computers.
    
    :param configuration_file: Path to the YAML configuration file.
    :param config_path: Path to the SSH config directory.
    :return: Formatted string with the results.
    """
    updates_needed={}
    # Convert to Path objects
    config_path = Path(config_path)
    result_msg = ""
        
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
        return False,result_msg + msg
                           
        
    # Check if each defined computer exists in AiiDA and is up-to-date
    defined_computers = config.get("computers", {})

    # Checking for old grants
    defined_grants = config['widgets']['grant']
    for computer in active_computers:
        if computer != 'localhost':
            its_grant = computer.split('_')[-1]
            if its_grant == computer:
                its_grant = 'notspecfied'
            if its_grant not in defined_grants:
                result_msg += f"⚠️ Computer '{computer}' is installed in AiiDA but its grant '{its_grant}' is not defined in the configuration file.<br>"
                updates_needed.setdefault('computers', {})[computer] = {'hide':True,'rename': False,'install':False}

    # Checking computers
    for comp, comp_data in defined_computers.items():
        full_comp = comp_data['setup']['label']
        if full_comp in active_computers:
            result_msg += f"✅⬜ Computer '{full_comp}' is already installed in AiiDA, checking for its configuration.<br>"
            is_up_to_date, msg = compare_computer_configuration(full_comp, comp_data)
            result_msg += msg
            if not is_up_to_date:  # Only add to updates_needed if not up-to-date
                updates_needed.setdefault('computers', {})[full_comp] = {'hide':True,'rename': True,'install':True}

        elif full_comp in not_active_computers:
            result_msg += f"⬜ Computer '{full_comp}' is listed but NOT active in AiiDA.<br>"
            updates_needed.setdefault('computers', {})[full_comp] = {'hide':False,'rename': True,'install':True}

        else: #here distinguish between all grants and selected grant
            result_msg += f"❌ Computer '{full_comp}' is completely missing from AiiDA.<br>"
            updates_needed.setdefault('computers', {})[full_comp] = {'hide':False,'rename': False,'install':True}

    # Checking codes
    defined_codes = config.get("codes", {})

    # Check if each defined code exists in AiiDA and is up-to-date
    # in the yaml configuration a code definition also include the computer
    
    # To do. loop on active codes with grant that is old, --> hide
    for _, code_data in defined_codes.items(): 
        computer = defined_computers[code_data['computer']]['setup']['label']
        computer_up_to_date = computer not in updates_needed.get('computers', {})
        code_label = f"{code_data['label']}@{computer}"

        # Default: No update needed
        msg = f"✅ Code {code_label} is already installed in AiiDA.<br>"

        if computer_up_to_date:  # Computer is up-to-date, check renaming needs
            code_pk = next((pk for codename, pk in active_codes if codename == code_label), None)
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
        

    return True,result_msg,updates_needed

def setup_computers(computers_to_setup,defined_computers):
    status = True
    for computer in computers_to_setup:
        print("CHECKING COMPUTER",computer)
        computer_name = computer.split('_')[0]
        _, _, grant = computer.partition('_') # gives '' if no _ is found
        print(f"🔄 Dealing with '{computer}'")
        config_computers = defined_computers.get(computer_name,{})
        status = setup_aiida_computer(computer, config_computers,hide=computers_to_setup[computer].get('hide',False),
                             torelabel=computers_to_setup[computer].get('rename',False),
                             install=computers_to_setup[computer].get('install',False),
                             grant=grant
                             )
    return status
def setup_codes(codes_to_setup,config):
    defined_codes = config.get("codes", {})
    uenvs=[]
    status = True
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
                            pktorelabel=codes_to_setup[full_code].get('rename',False),
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
        print(f"🔍 Checking UENV repository status on {remotehost}")
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
            print(f"✅ UENV repo is available on {remotehost}.")

    # Step 2: Get the list of images available to the user
    available_images = {}
    for remotehost in hosts:
        print(f"🔍 Fetching available UENV images on {remotehost} for the user ")
        command = ["ssh", remotehost, "uenv", "image", "ls"]
        command_out, command_ok = run_command(command)
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
        f"scp {config_files}/mps-wrapper.sh {remotehost}:/users/{cscs_username}/bin/",
        f"scp -r {config_files}/cp2k {remotehost}:/users/{cscs_username}/src/"
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

def setup_phonopy(cscs_username, remotehost):
    conda_init = f"source /users/{cscs_username}/miniconda3/bin/activate"
    commands = [
        ["ssh", remotehost, "if [ ! -f Miniconda3-latest-Linux-aarch64.sh ]; then wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh; fi"],
        ["ssh", remotehost, f"if [ ! -d /users/{cscs_username}/miniconda3 ]; then bash Miniconda3-latest-Linux-aarch64.sh -b -p /users/{cscs_username}/miniconda3; fi"],
        ["scp", f"{config_files}/bashrc_template", f"{remotehost}:/users/{cscs_username}"],
        ["ssh", remotehost, "mv .bashrc .old_bashrc"],
        ["ssh", remotehost, "mv bashrc_template .bashrc"],
        ["ssh", remotehost, f"bash -l -c '{conda_init} && conda env list | grep -q \"^phonopy \" && echo \"Environment exists\" || conda create -n phonopy -c conda-forge phonopy seekpath'"],
    ]
    
    for command in commands:
        command_out, command_ok = run_command(command)
        if not command_ok:
            print(f"❌ Failed to execute: {' '.join(command)}. Exiting, ask for help.")
            return False
    
    _ = setup_codes(["phonopy.yml"])
    return True

def setup_critic2(cscs_username, qe_uenv, remotehost,python_version):
    required_packages = ["pymatgen", "cloudpickle", "scikit-image"]
    
    env_setup_command = ["ssh", remotehost, f"conda env list | grep -q '^py39 ' || conda create -n py39 -c conda-forge python={python_version} {' '.join(required_packages)} -y"]
    run_command(env_setup_command)
    
    check_packages_cmd = ["ssh", remotehost, "conda activate py39 && conda list | awk '{print $1}'"]
    output, _ = run_command(check_packages_cmd)
    
    installed_packages = output.split() if output else []
    missing_packages = [pkg for pkg in required_packages if pkg not in installed_packages]
    
    if missing_packages:
        install_cmd = ["ssh", remotehost, f"conda activate py39 && conda install -n py39 -c conda-forge {' '.join(missing_packages)} -y"]
        run_command(install_cmd)
    
    #_ = setup_codes(["python.yml"])
    
    commands = [
        ["ssh", remotehost, "if [ ! -d 'critic2' ]; then git clone https://github.com/aoterodelaroza/critic2.git; fi"],
        ["ssh", remotehost, f"if [ ! -f /users/{cscs_username}/critic2/build/src/critic2 ]; then cd critic2 && mkdir -p build && cd build && uenv run {qe_uenv} cmake .. && uenv run {qe_uenv} make; fi"],
        ["ssh", remotehost, f"ls /users/{cscs_username}/critic2/build/src/critic2"]
    ]
    
    for command in commands:
        command_out, command_ok = run_command(command)
        if not command_ok:
            print(f"❌ Failed to execute: {' '.join(command)}. Exiting, ask for help.")
            return False
    
    #_ = setup_codes(["critic2.yml"])
    return True


def execute_special_setup(setup_name, cscs_username, remotehost, qe_uenv="", python_version=""):
    """Execute a setup sequence from YAML file."""
    yaml_commands = load_yaml_commands()
    
    if setup_name not in yaml_commands["special_commands"]:
        print(f"❌ Setup '{setup_name}' not found in YAML file. Exiting.")
        return False
    
    conda_init = yaml_commands.get("conda_init", "source /users/{cscs_username}/miniconda3/bin/activate").format(cscs_username=cscs_username)
    
    # Run environment setup commands if they exist
    env_setup_commands = yaml_commands.get("env_setup_commands", {}).get(setup_name, [])
    for entry in env_setup_commands:
        formatted_command = entry["command"].format(
            cscs_username=cscs_username,
            remotehost=remotehost,
            qe_uenv=qe_uenv,
            python_version=python_version,
            conda_init=conda_init
        )
        
        ssh_command = ["ssh", remotehost, formatted_command] if entry["type"] == "ssh" else formatted_command.split()
        output, success = run_command(ssh_command)
        if not success:
            print(f"❌ Failed to execute: {formatted_command}. Exiting, ask for help.")
            return False
    
    # Run special setup commands
    commands = yaml_commands["special_commands"][setup_name]
    for entry in commands:
        if isinstance(entry, dict):
            command_type = entry.get("type", "shell")
            command = entry.get("command", "")
        else:
            command_type = "shell"
            command = entry
        
        formatted_command = command.format(
            cscs_username=cscs_username,
            remotehost=remotehost,
            qe_uenv=qe_uenv,
            python_version=python_version,
            conda_init=conda_init
        )
        
        ssh_command = ["ssh", remotehost, formatted_command] if command_type == "ssh" else formatted_command.split()
        output, success = run_command(ssh_command)
        if not success:
            print(f"❌ Failed to execute: {formatted_command}. Exiting, ask for help.")
            return False
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