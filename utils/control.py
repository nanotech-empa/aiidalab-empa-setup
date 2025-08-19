import os
from copy import deepcopy
from itertools import product
import ipywidgets as ipw
from IPython.display import display
import time
from .string_utils import *
from .repo_utils import *
from .aiida_and_ssh_utils import *
from datetime import datetime,timedelta


# Check repository of config files
def check_repository():
    """Check if the repository exists and pull the latest changes."""
    msg = "<b style='color:green;'>✅ Repository is up to date.</b>"
    # Ensure the repository exists
    if not os.path.exists(GIT_REPO_PATH):
        msg = "<b style='color:orange;'>⚠️ Repository updated. Please inspect and then apply changes.</b>"
        if not clone_repository():
            return False,"<b style='color:red;'>❌ Failed to clone the repository. Please check your configuration.</b>"
    
    local_commit = get_local_commit()
    remote_commit = get_latest_remote_commit()
    
    if not local_commit or not remote_commit:
        return False,"<b style='color:red;'>❌ Unable to check for updates.</b>"

    if local_commit != remote_commit:
        if not pull_latest_changes():
            return False,"<b style='color:red;'>❌ Failed to update the repository.</b>"
        else:
            msg = "<b style='color:orange;'>⚠️ Repository updated. Please apply changes.</b>"
    return True,msg  

def get_config(file_path='/home/jovyan/opt/aiidalab-alps-files/config.yml', config_widgets={}):
    """Get the configuration from the YAML file."""
    # Verifica che tutti i widget siano selezionati
    for key in config_widgets:
        if config_widgets[key].value == "select":
            return False,f"<b style='color:red;'>❌please select {key}</b>", {}

    # Verifica lo stato del repository
    status_ok, msg = check_repository()
    if not status_ok:
        return status_ok,msg, {}

    # Carica lo YAML
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)

    variables = data.get("variables", {})
    if 'timestamp' in variables and variables['timestamp'] == 'now':
        variables['timestamp'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
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

    return True,'', data



def check_for_updates(config,selected_grant):
    """Checks teh config file."""    
    status,msg,updates_needed = process_aiida_configuration(config, config_path,selected_grant)
    if not status:
        return msg,{}
    if not updates_needed:
        return "<b style='color:green;'>✅ Your configuration is up to date.</b>",{}
    else:
        return msg,updates_needed
    
def process_aiida_configuration(config, config_path,selected_grant):
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
    # Build valid combinations
    valid_computer_grants =  [f"{name}_{grant}" for name, data in defined_computers.items() for grant in data['grants']]
    selected_computer_grant = [f"{name}_{grant}" for name, data in defined_computers.items() for grant in data['grants'] if grant == selected_grant]

    # Add special standalone entries
    valid_computer_grants += ["localhost"]
    # Checking for old grants
    
    defined_grants = config['widgets']['grant']
    defined_grants.remove('select')
    for computer in active_computers:
        if computer not in valid_computer_grants:
            result_msg += f"⚠️ Computer '{computer}' is installed in AiiDA but  is not foreseen in the configuration file.<br>"
            updates_needed.setdefault('computers', {})[computer] = {'hide':True,'rename': False,'install':False}

    # Checking computers
    for comp, comp_data in defined_computers.items():
        # full_comp = daint_lp83 since in the yml is daint_{grant}
        full_comp = comp_data['setup']['label']
        if full_comp in active_computers:
            result_msg += f"✅⬜ Computer '{full_comp}' is already installed in AiiDA, checking for its configuration.<br>"
            is_up_to_date, msg = compare_computer_configuration(full_comp, comp_data)
            result_msg += msg
            if not is_up_to_date:  # Only add to updates_needed if not up-to-date
                install = full_comp in selected_computer_grant
                updates_needed.setdefault('computers', {})[full_comp] = {'hide':True,'rename': True,'install':install}

        elif full_comp in not_active_computers:
            result_msg += f"⬜ Computer '{full_comp}' is listed but NOT active in AiiDA.<br>"
            install = full_comp in selected_computer_grant
            updates_needed.setdefault('computers', {})[full_comp] = {'hide':False,'rename': True,'install':install}

        else: #here distinguish between all grants and selected grant
            install = full_comp in selected_computer_grant
            if install:
                result_msg += f"❌ Computer '{full_comp}' is completely missing from AiiDA.<br>"
                updates_needed.setdefault('computers', {})[full_comp] = {'hide':False,'rename': False,'install':install}

    # Checking codes
    defined_codes = config.get("codes", {})

    # Check if each defined code exists in AiiDA and is up-to-date
    # in the yaml configuration a code definition also include the computer
 
    # Hide unclassified codes

    # hide and rename codes of old computers
    for codename, codecomputer, code_pk in active_codes:
        code_label = f"{codename}@{codecomputer}"
        if codecomputer not in valid_computer_grants:
            result_msg += f"⚠️ Code '{codename}' is installed in AiiDA but its computer/grant is not defined in the configuration file.<br>"
            updates_needed.setdefault('codes', {})[code_label] = {'hide':code_pk,'rename':code_pk,'install':False}
    
    
    for code_key, code_data in defined_codes.items(): 
        computer = defined_computers[code_data['computer']]['setup']['label']
        install = computer in selected_computer_grant
        computer_will_be_outdated = computer in updates_needed.get('computers', {}) and not updates_needed['computers'][computer].get('install',False)
        computer_will_be_installed = computer in updates_needed.get('computers', {}) and  updates_needed['computers'][computer].get('install',False)
        computer_up_to_date = computer in active_computers and not computer_will_be_installed
        code_label = f"{code_data['label']}@{computer}"
        code_pk_active = next((pk for codename,codecomputer, pk in active_codes if f"{codename}@{codecomputer}" == code_label), None)
        code_pk_not_active = next((pk for codename,codecomputer, pk in not_active_codes if f"{codename}@{codecomputer}" == code_label), None)

        # Default: No update needed but check for uenv
    
        msg = f"✅ Code {code_label} is already installed in AiiDA.<br>"
        
        #check for all codes independently from the selected grant and install in case of matching grant
        if computer_will_be_outdated: # Computer is not up-to-date, check active and non active codes
            if code_pk_active is not None: # the code is already present and active
                updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'rename': code_pk_active,'hide':True,'install':False}
                msg = f"⚠️ Code {code_label} is already installed  in AiiDA but on a old computer. Will be renamed and reinstalled.<br>"
            elif code_pk_not_active is not None: 
                updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'rename': code_pk_not_active,'hide':False,'install':False}
                msg = f"⚠️ Code {code_label} is already installed  in AiiDA,not active and on a old computer. Will be renamed and reinstalled.<br>"
        elif computer_will_be_installed: # Computer is not present, and will be installed
            if install:
                updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'rename': False,'install':True}
                msg = f"⬜ Code {code_label} will be installed  {computer} will be installed.<br>"
        elif computer_up_to_date: # Computer is present and up-to-date
            if install:
                if code_pk_active is not None: # the code is already present and active
                    codes_equal,msg = compare_code_configuration(code_label,code_data)
                    if not codes_equal: # but outdated
                        updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'rename': code_pk_active,'install':True}
                        msg = f"⬜ Code {code_label} will be installed  {computer} is present.<br>"
                    else:
                        updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'checkuenv': True,'install':False}
                        msg = f"✅ Code {code_label} is already installed in AiiDA and up-to-date we will check if uenv is present.<br>"
                elif code_pk_not_active is not None: # the code is already present but not active
                    updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'rename': code_pk_active,'install':True}
                    msg = f"⬜ Code {code_label} will be installed  {computer} is present the old non active code will be renamed.<br>"
                else:
                    updates_needed.setdefault('codes', {})[code_label] = {'code_key': code_key,'rename': False,'install':True}
                    msg = f"⬜ Code {code_label} will be installed  {computer} is present.<br>"
                
        
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
        hide=codes_to_setup[full_code].get('hide',False)
        pktorelabel=codes_to_setup[full_code].get('rename',False)
        install=codes_to_setup[full_code].get('install',False)
        checkuenv = codes_to_setup[full_code].get('checkuenv',False)
        #code = full_code.split('@')[0].split('-')[0] # pw
        code = codes_to_setup[full_code].get('code_key')
        code_data={}
        
        if install or checkuenv:
            code_data = defined_codes[code]
            computer = code_data['computer']
            hostname = config['computers'][computer]['setup']['hostname']
            prepend_text = code_data.get("prepend_text", "")
            match = re.search(r"#SBATCH --uenv=([\w\-/.:]+)", prepend_text)
            if match:
                uenv_value = match.group(1)  # Extract matched value
                print(f"⬜  Need uenv: {uenv_value} for '{full_code}'")
                if (hostname,uenv_value) not in uenvs:
                    uenvs.append((hostname,uenv_value))
            else:
                print(f"✅ No uenv needed for '{full_code}'")
            
        status = setup_aiida_code(full_code, code_data,hide=hide,
                            pktorelabel=pktorelabel,
                            install=install)
        
            

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
        #print(extract_first_column(command_out))
        if not command_ok:
            print(f"❌ Failed to fetch UENV images on {remotehost}. Exiting.")
            return False
        available_images.setdefault(remotehost,{})['user'] = extract_first_column(command_out)

        # Step 3: Get the list of all images available on the system
        print("🔍 Fetching available UENV images (system-wide)")
        
        command = ["ssh", remotehost, "uenv", "image", "find"]
        command_out, command_ok = run_command(command)
        #print(extract_first_column(command_out))
        if not command_ok:
            print("❌ Failed to fetch system-wide UENV images. Exiting.")
            return False
        available_images.setdefault(remotehost,{})['host'] = extract_first_column(command_out)
        # Get the list of all images available on service:: (if any)
        print("🔍 Fetching available UENV images on service::")        
        command = ["ssh", remotehost, "uenv", "image", "find", "service::"]
        command_out,command_ok = run_command(command)
        #print(extract_first_column(command_out))
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
        elif env in available_images[remotehost]['service']:
            print(f"✅ Image '{env}' is available in the service repo on {remotehost}. Pulling from service::...")
            command = ["ssh", remotehost, "uenv", "image", "pull", f"service::{env}"]
            command_out, command_ok = run_command(command)
        else:
            print(f"❌ Image '{env}' is not available anywhere on {remotehost}! Manual intervention needed.")
            return False

    print("✅ UENV management complete.")
    return True
