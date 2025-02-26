import os
import re
from pathlib import Path
import shutil
import subprocess
from datetime import datetime
import yaml
import ipywidgets as ipw
from IPython.display import display
import time
from datetime import datetime,timedelta
from aiida import load_profile
from aiida.orm import load_node
from aiida.orm import QueryBuilder, WorkChainNode, StructureData, Node

# labels for paths
alps_files ='' # REMOVE
repo_name = "aiidalab-alps-files"
home_dir = Path("/home/jovyan")  # Explicitly set /home/jovyan
target_dir = home_dir / "opt"
config_files = target_dir / repo_name  # Ensure `repo_name` is defined
config_path = home_dir / ".ssh" 
configuration_file = config_files / "config.yml"
GIT_REPO_PATH = config_files
GIT_URL = "https://github.com/nanotech-empa/aiidalab-alps-files.git"  # files needed on daint
GIT_REMOTE = "origin"
BRANCH = "main"

# labels to rename old host and codes
def relabel(label):
    """Assumes that in case @ is present it is a code and keeps only the portion preceding @ """
    before_at, at, after_at = label.partition('@')
    return datetime.now().strftime("%Y%m%d%H%M")+'_'+before_at
        

def clone_repository():
    """Clone the repository if it does not exist."""
    try:
        #print("🔄 Cloning repository...")
        result = subprocess.run(
            ["git", "clone", "-b", BRANCH, GIT_URL, GIT_REPO_PATH],
            capture_output=True,
            text=True,
            check=True
        )
        return True  # Repo was successfully cloned
    except subprocess.CalledProcessError:
        return False  # Failed to clone

def get_latest_remote_commit():
    """Fetch the latest commit hash from the remote repository."""
    try:
        result = subprocess.run(
            ["git", "ls-remote", GIT_REMOTE, BRANCH],
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.split()[0] if result.stdout else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def get_local_commit():
    """Get the latest local commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def pull_latest_changes():
    """Pull the latest changes from the remote repository."""
    try:
        result = subprocess.run(
            ["git", "pull", GIT_REMOTE, BRANCH],
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True
        )
        return "Already up to date" not in result.stdout
    except subprocess.CalledProcessError:
        return False
def remove_placeholders(str1, str2, ignored_patterns=["cscsusername", "cscsaccount"]):
    """
    Removes one of the given placeholders if it appears in exactly one of the strings.
    The corresponding portion in the other string (up to the next '/', '\n', or space) is also removed.

    :param str1: First string.
    :param str2: Second string.
    :param ignored_patterns: List of placeholders to ignore if they appear in exactly one of the strings.
    :return: Modified versions of str1 and str2.
    """
    
    for ignored_pattern in ignored_patterns:
        # Check if one string contains the placeholder but the other does not
        if ignored_pattern in str1 and ignored_pattern not in str2:
            username_str, other_str = str1, str2
        elif ignored_pattern in str2 and ignored_pattern not in str1:
            username_str, other_str = str2, str1
        else:
            continue  # If neither or both contain it, move to the next placeholder

        # Find the position of the placeholder in the string that contains it
        pos = username_str.find(ignored_pattern)
        if pos == -1:
            continue  # Should never happen, but just in case

        # Remove the placeholder from the string that contains it
        modified_username_str = username_str[:pos] + username_str[pos+len(ignored_pattern):]

        # Remove the corresponding part from the other string up to the next '/', '\n', ' ' or end
        match = re.search(r'[/\n ]', other_str[pos:])  # Find next '/' or '\n' after `pos`
        if match:
            end_pos = pos + match.start()  # Absolute position in the string
            modified_other_str = other_str[:pos] + other_str[end_pos:]
        else:
            modified_other_str = other_str[:pos]  # No match found → Remove everything till end

        # Update str1 and str2 for further processing
        str1, str2 = modified_username_str, modified_other_str
    return str1, str2

def normalize_text(text):
    """
    - Removes extra empty lines.
    - Ensures a maximum of one space between words (but keeps line breaks).
    - Removes everything after '#SBATCH --account=' up to the newline.
    """
    if text is None:
        return ""  # Treat None as empty string

    # Normalize each line: strip leading/trailing spaces and reduce multiple spaces to one
    lines = [re.sub(r"\s+", " ", line.strip()) for line in text.splitlines() if line.strip()]

    # Replace '#SBATCH --account=' followed by anything with just '#SBATCH --account='
    #lines = [re.sub(r"(#SBATCH --account=).*", r"\1", line) for line in lines]

    # Join lines back together while preserving newlines
    return "\n".join(lines)

def compare_computer_configuration(computer_name, stored_computer_data):
    """
    Compares the setup and config of a computer in AiiDA against the stored values in config.yml.

    :param computer_name: The name of the computer in AiiDA.
    :param config_file: Path to config.yml.
    :return: A formatted string with the comparison results.
    """

    # Get the stored setup and config for the given computer
    
    stored_setup = stored_computer_data.get("setup", {})
    stored_config = stored_computer_data.get("config", {})

    if not stored_setup or not stored_config:
        return f"❌ Computer '{computer_name}' not found in config.yml!"

    # Step 2: Export the current setup and config from AiiDA
    setup_export_file = "setup.yml"
    config_export_file = "config.yml"

    try:
        subprocess.run(["verdi", "computer", "export", "setup", computer_name, setup_export_file], check=True,
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.DEVNULL)
        subprocess.run(["verdi", "computer", "export", "config", computer_name, config_export_file], check=True,
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        return False, f"❌ Error exporting AiiDA computer setup/config: {e.stderr}"

    # Step 3: Load exported YAML files
    with open(setup_export_file, "r") as file:
        exported_setup = yaml.safe_load(file)

    with open(config_export_file, "r") as file:
        exported_config = yaml.safe_load(file)

    #setup differences
    for entry in stored_setup.keys():  
        str1,str2 = remove_placeholders(normalize_text(str(stored_setup[entry])), 
                                       normalize_text(str(exported_setup[entry])))
        if not str1 == str2:
            return False,f"⚠️ **Setup Differences:**\n"
    
    for entry in stored_config.keys(): 
        str1,str2 = remove_placeholders(normalize_text(str(stored_config[entry])), 
                                       normalize_text(str(exported_config[entry])))        
        if not str1==str2:
            return False,"⚠️ **Config Differences:**\n"        


    return True, "✅ No differences found! The stored configuration matches AiiDA.\n"


def compare_code_configuration(stored_code_data):
    computer = stored_code_data['computer']
    code_label = stored_code_data['label']
    try:
        subprocess.run(["verdi", "code", "export", f"{code_label}@{computer}", "export.yml"], check=True,
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        return False, f"❌ Error exporting AiiDA code setup: {e.stderr}"

    # Step 3: Load exported YAML files
    with open("export.yml", "r") as file:
        exported_setup = yaml.safe_load(file)

    #setup differences
    for entry in stored_code_data.keys():  
        str1,str2 = remove_placeholders(normalize_text(str(stored_code_data[entry])), 
                                       normalize_text(str(exported_setup[entry])))
        if not str1 == str2:
            return False,f"⚠️ **Setup Differences:**\n"
        
    return True, f"✅ No differences found! The stored configuration for {code_label} matches AiiDA.\n"

def aiida_computers():
    result_msg = ""
    active_computers = set()
    not_active_computers = set()

    try:
        result = subprocess.run(
            ["verdi", "computer", "list", "-a"],
            capture_output=True,
            text=True,
            check=True
        )

        for line in result.stdout.splitlines():
            stripped_line = line.strip()
            if stripped_line.startswith("* "):  # Active  computers
                active_computers.add(stripped_line[2:])  # Remove "* "
            elif stripped_line and not stripped_line.startswith("Report:"):  # Inactive computers
                not_active_computers.add(stripped_line)

    except subprocess.CalledProcessError as e:
        return False, f"❌ Error running 'verdi computer list -a': {e.stderr}",active_computers,not_active_computers

    result_msg += f"✅ Active AiiDA computers: {', '.join(active_computers)}\n"
    result_msg += f"⬜ Not active AiiDA computers: {', '.join(not_active_computers)}\n"
    return True,result_msg,active_computers,not_active_computers

def aiida_codes():
    result_msg = ""
    all_codes = set()
    codes = set()

    try:
        result = subprocess.run(["verdi", "code", "list", "-a"], capture_output=True, text=True, check=True)
        lines = result.stdout.splitlines()
        all_codes = {line.split()[0] for line in lines if "@" in line}
    except subprocess.CalledProcessError as e:        
        return False, f"❌ Error running 'verdi code list -a': {e.stderr}",set(),set()
    
    
    try:
        result = subprocess.run(["verdi", "code", "list"], capture_output=True, text=True, check=True)
        lines = result.stdout.splitlines()
        codes = {line.split()[0] for line in lines if "@" in line}
    except subprocess.CalledProcessError as e:        
        return False, f"❌ Error running 'verdi code list': {e.stderr}",set(),set() 
    
    not_active_codes = all_codes - codes
        
    result_msg += f"✅ Active AiiDA codes: {', '.join(codes)}\n"
    result_msg += f"⬜ Not active AiiDA codes: {', '.join(not_active_codes)}\n"
    return True,result_msg,codes,not_active_codes

def check_ssh_config(config_path, config_from_yaml):
    config_file = config_path / "config"
    
    # Read the content of the config file
    try:
        with open(config_file, "r") as f:
            config_content = f.read()
    except FileNotFoundError:
        return False, f"Config file {config_file} not found. I will create it.\n"
    
    for computer, details in config_from_yaml.items():
        setup = details.get("setup", {})
        config = details.get("config", {})

        hostname = setup.get("hostname")
        proxy_jump = "Host " + config.get("proxy_jump", "")

        # Check if hostname is in the config file
        hostname_check = hostname in config_content if hostname else False
        proxy_check = proxy_jump in config_content if proxy_jump else True  # Skip if empty

        if not (hostname_check and proxy_check):
            return False,f"{hostname} not properly configured in .ssh/config. Overwriting config"
    
    return True, "The .ssh/config seems to be OK"

def update_ssh_config(config_path,rename=True):
    
    # Ensure config_path exists
    config_path.mkdir(parents=True, exist_ok=True)

    # Define file paths
    config_file = config_path / "config"
    old_config_file = config_path / relabel("config") 

    result_msg = ""
    
    ssh_config_data = config.get("ssh_config", "")  
    if rename:
        shutil.move(config_file, old_config_file)
        result_msg += f"✅ Renamed {config_file} → {old_config_file}\n"

    with open(config_file, "w") as file:
        file.write(ssh_config_data + "\n")  # Ensure a newline at the end

    result_msg += f"✅ Created new SSH config at {config_file}\n"
    return True,result_msg
    

def process_aiida_configuration(configuration_file, config_path):
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
    
    # Check ssh_config
    config_ok,msg = check_ssh_config(config_path, config['computers'])
    result_msg +=msg
    if not config_ok:
        if 'Overwriting' in msg:
            updates_needed.setdefault('ssh_config', {})['rename'] =  True
        else:
            updates_needed.setdefault('ssh_config', {})['rename'] =  False
        
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

    # Checking computers
    for comp, comp_data in defined_computers.items():
        if comp in active_computers:
            result_msg += f"✅ Computer '{comp}' is already installed in AiiDA.\n"
            is_up_to_date, msg = compare_computer_configuration(comp, comp_data)
            result_msg += msg
            if not is_up_to_date:  # Only add to updates_needed if not up-to-date
                updates_needed.setdefault('computers', {})[comp] = {'rename': True}

        elif comp in not_active_computers:
            result_msg += f"⬜ Computer '{comp}' is listed but NOT active in AiiDA.\n"
            updates_needed.setdefault('computers', {})[comp] = {'rename': True}

        else:
            result_msg += f"❌ Computer '{comp}' is completely missing from AiiDA.\n"
            updates_needed.setdefault('computers', {})[comp] = {'rename': False}

    # Checking codes
    defined_codes = config.get("codes", {})

    for code, code_data in defined_codes.items():
        computer = code_data['computer']
        computer_up_to_date = computer not in updates_needed
        code_label = code_data['label']

        # Default: No update needed
        msg = f"✅ Code {code_label} is already installed in AiiDA.\n"

        if computer_up_to_date:  # Computer is up-to-date, check renaming needs
            if f"{code_label}@{computer}" in active_codes:
                if not compare_code_configuration(code_data):
                    updates_needed.setdefault('codes', {})[code] = {'rename': True}
                    msg = f"⚠️ Code {code} is already installed in AiiDA but is old. Will be renamed and reinstalled\n"
            elif f"{code_label}@{computer}" in not_active_codes:  # Rare case: inactive code with the same name
                updates_needed.setdefault('codes', {})[code] = {'rename': True}
                msg = f"⚠️ Code {code_label} is already installed (not active) in AiiDA but is old. Will be renamed and reinstalled\n"
            else:
                updates_needed.setdefault('codes', {})[code] = {'rename': False}
                msg = f"⬜ Code {code_label} will be installed  {computer} is present.\n"
        else: #I will install the computer thus teh code and teh code does not have to be renamed
            updates_needed.setdefault('codes', {})[code] = {'rename': False}
            msg = f"⬜ Code {code_label} will be installed after installation of {computer}. No need to rename.\n"

        result_msg += msg   
    # To do: Check if cusntom app installations are needed
        

    return True,result_msg,updates_needed

def check_for_updates():
    """Check if there is a new update available and pull changes if necessary."""
    
    # Ensure the repository exists
    if not os.path.exists(GIT_REPO_PATH):
        if not clone_repository():
            return "<b style='color:red;'>❌ Failed to clone the repository. Please check your configuration.</b>"
    
    local_commit = get_local_commit()
    remote_commit = get_latest_remote_commit()
    
    if not local_commit or not remote_commit:
        return "<b style='color:red;'>❌ Unable to check for updates.</b>",{}

    if local_commit != remote_commit:
        if not pull_latest_changes():
            return "<b style='color:red;'>❌ Failed to update the repository.</b>",{}
    
    status,msg,updates_needed = process_aiida_configuration(configuration_file, config_path)
    if not status:
        return msg,{}
    if not updates_needed:
        return "<b style='color:green;'>✅ Your configuration is up to date.</b>",{}
    else:
        return msg,updates_needed

def process_yml_files(yml_files):
    """
    Given a list of YAML file names, this function:
    1) Extracts 'label' and 'computer' values from the YAML file.
    2) Executes 'verdi code export {label}@{computer} e_code.yml'.
    3) Replaces occurrences of 'cpi' with 'cscsusername' in e_code.yml.
    4) Renames e_code.yml to the original YAML filename.

    Args:
        yml_files (list of str): List of target YAML file names.
    """
    for yml_file in yml_files:
        try:
            # Load YAML file to extract label and computer
            with open(yml_file, 'r') as file:
                config = yaml.safe_load(file)

            label = config.get("label")
            computer = config.get("computer")

            if not label or not computer:
                print(f"Skipping {yml_file}: 'label' or 'computer' not found.")
                continue  # Skip this file if the required keys are missing

            exported_file = f"e_{yml_file}"

            # Step 1: Execute 'verdi code export'
            command = f"verdi code export {label}@{computer} {exported_file}"
            subprocess.run(command, shell=True, check=True)
            print(f"Exported: {exported_file}")

            # Step 2: Replace 'cpi' with 'cscsusername' in the exported file
            with open(exported_file, 'r') as file:
                content = file.read()

            content = content.replace('cpi', 'cscsusername')

            with open(exported_file, 'w') as file:
                file.write(content)

            print(f"Updated {exported_file} with 'cscsusername'.")

            # Step 3: Rename the exported file to match the original YAML filename
            subprocess.run(f"mv {exported_file} {yml_file}", shell=True, check=True)
            print(f"Renamed {exported_file} to {yml_file}.")

        except Exception as e:
            print(f"Error processing {yml_file}: {e}")


def run_command(command, ssh=False, max_retries=5, remotehost=None):
    """
    Run a shell command locally or over SSH, capturing output and handling errors.
    If the error contains 'Connection closed by remote host', retries up to 5 times with a 5-second wait.
    
    Args:
        command (str): The command to execute.
        ssh (bool): Whether to run the command over SSH.
        max_retries (int): Maximum number of retries on connection failure.
        remotehost (str): The remote host for SSH (required if ssh=True).
    
    Returns:
        tuple: (command output as string, success as bool)
    """
    if ssh and not remotehost:
        raise ValueError("SSH is enabled but 'remotehost' is not provided.")

    attempts = 0
    while attempts < max_retries:
        if ssh:
            full_command = f"ssh {remotehost} '{command}'"
        else:
            full_command = command

        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✅ Command successful: {full_command}")
            return result.stdout.strip(), True

        stderr_output = result.stderr.strip()
        print(f"❌ Attempt {attempts + 1} failed: {full_command}")
        print("STDERR:", stderr_output)

        # Check for specific error
        if "Connection closed by remote host" in stderr_output:
            attempts += 1
            if attempts < max_retries:
                print(f"🔄 Retrying in 5 seconds... (Attempt {attempts}/{max_retries})")
                time.sleep(5)
                continue  # Retry the command
            else:
                print("❌ Maximum retries reached. Exiting.")
                return "", False
        else:
            return stderr_output, False  # Fail immediately if it's not a connection error

    return "", False  # Should not reach here



# clone repository
#def clone_repo(target_dir,repo_name):
#    repo_path = os.path.join(target_dir, repo_name)
#
#    if not os.path.isdir(repo_path):  # Check if the directory exists
#        #print(f"Cloning {GIT_URL} into {repo_path}...")
#        commnad_out,command_ok = run_command(f"cd {target_dir} && git clone {GIT_URL}", ssh=False)
#        if not command_ok:
#            return
#    else:
#        print(f"Repository {repo_name} exists, pulling latest changes...")
#        commnad_out,command_ok = run_command(f"cd {repo_path} && git reset --hard HEAD && git pull", ssh=False)
#        if not command_ok:
#            return
#    return

# Function to load YAML file as a dictionary
def load_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

# widgets
# Create input fields
# Define a common style with enough space for descriptions
style = {'description_width': '150px'}  # Adjust as needed

# Create input fields with adjusted description width
username_widget = ipw.Text(
    value='',
    description='CSCS Username:',
    placeholder='Enter your username',
    layout=ipw.Layout(width='400px'),  # Total width
    style=style
)

account_widget = ipw.Dropdown(
    value='s1267',
    options=['s1267', 's1276'],
    description='CSCS Account:',
    layout=ipw.Layout(width='400px'),
    style=style
)


def config_command(cscs_username):
    command = f"""
if grep -q 'daint.alps.cscs.ch' {config_path} 2>/dev/null; then
    echo 'No changes needed for .ssh/config: daint.alps.cscs.ch already present in {config_path}';
else
    echo 'need to update config'
    if [ ! -f {config_path} ]; then
        cp {config_source} {config_path};
    else
        if grep -q 'ela' {config_path}; then
            echo 'ela present'
            cat {config_without_ela} {config_path} > {config_path}.tmp && mv {config_path}.tmp {config_path};
        else
            echo 'ela not present'
            cat {config_source} {config_path} > {config_path}.tmp && mv {config_path}.tmp {config_path};
        fi
    fi
    sed -i 's/cscsusername/{cscs_username}/g' {config_path}
    echo 'Updated {config_path} successfully!';
fi
"""
    return command

def update_yml_files(cscs_username, cscs_account, alps_files, yml_files):
    """Rename files, check for placeholders, and update content dynamically."""
    
    for file in yml_files:  # Use the provided yml_files list instead of a hardcoded file list
        file_path = os.path.join(alps_files, file)

        # Ensure file exists before proceeding
        if not os.path.exists(file_path):
            print(f"⚠️ Skipping {file}: File not found.")
            continue

        # Check if file contains `cscsusername` or `cscsaccount`
        with open(file_path, "r") as f:
            content = f.read()

        # Generate sed commands dynamically if placeholders exist
        sed_commands = []
        if "cscsusername" in content:
            sed_commands.append(f"sed -i 's/cscsusername/{cscs_username}/g' {file_path}")
        if "cscsaccount" in content:
            sed_commands.append(f"sed -i 's/cscsaccount/{cscs_account}/g' {file_path}")

        # Execute sed commands if needed
        if sed_commands:
            print(f"🔍 Updating {file}...")
            for cmd in sed_commands:
                subprocess.run(cmd, shell=True)

        # Rename file if it contains `cscsusername` in the filename
       #if "cscsusername" in file:
            #new_file_name = file.replace("cscsusername", cscs_username)
            #new_file_path = os.path.join(alps_files, new_file_name)
            #os.rename(file_path, new_file_path)
            #print(f"📂 Renamed {file} → {new_file_name}")

    print("✅ File updates complete.")


def check_install_computer():
    install_computer = True

    # Check if the computer exists
    #computer_exists_cmd = f"verdi computer list -a | grep -q '{new_host_label}'"
    process = subprocess.Popen("verdi computer list -a", shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    stdout, _ = process.communicate()
    
    computers = stdout.splitlines()
    
    if any(re.search(rf"\b{new_host_label}\b", line) for line in computers):
        print(f"✅ Exact match found: {new_host_label} Exporting configuration..")

        #subprocess.run(f"verdi computer export config {new_host_label} config.yml", shell=True, check=True)
        #subprocess.run(f"verdi computer export setup {new_host_label} setup.yml", shell=True, check=True)
        run_command(f"verdi computer export config {new_host_label} config.yml", ssh=False)
        run_command(f"verdi computer export setup {new_host_label} setup.yml", ssh=False)
        # Load and compare YAML files
        config_match = load_yaml(config_file) == load_yaml(ref_config)
        setup_match = load_yaml(setup_file) == load_yaml(ref_setup)

        if config_match and setup_match:
            print("✅ YAML files are equivalent, no need to reinstall the computer")
            install_computer = False
        else:
            print("❌ YAML files are different!")
        run_command("rm -rf config.yml setup.yml",ssh=False)
    else:
        print(f"Computer '{new_host_label}' not found. Skipping comparison.")
    return install_computer

# do first ssh connection /check ssh connection

def set_ssh(cscs_username):

    command = f"ls /users/{cscs_username}"
    commnad_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    if command_ok :
        return True

    # Loop over ela and daint hosts and add to known_hosts
    print(f"Adding {proxy} to known_hosts...")
    ssh_keyscan_command = f"ssh-keyscan -H {proxy} >> ~/.ssh/known_hosts"
    run_command(ssh_keyscan_command,ssh=False)

    print(f"Adding {remotehost} to known_hosts...")
    ssh_keyscan_command = f"ssh {proxy} ssh-keyscan -H {remotehost} >> ~/.ssh/known_hosts"
    run_command(ssh_keyscan_command,ssh=False)
    #run_command(ssh_keyscan_command,ssh=False)

    # check if ssh works
    commnad_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
    return command_ok

# relabel old daint if needed, setup new alps, and hide old codes

def setup_new_alps():
    # relabel old computers
    relabel_command = f"""if verdi computer list  | grep -q '{new_host_label}'; then 
        verdi computer relabel {new_host_label} {relabeled_host} && 
        verdi computer disable {relabeled_host} aiida@localhost; 
    fi"""

    # Hide old codes that will be already @relabeled_host 
    hide_code_command = f"verdi code list | awk 'NR>2 {{print $1}}' | grep '@{relabeled_host}' | xargs -r -I {{}} verdi code hide {{}}"

    # setup ALPS
    alps_setup_command = f"verdi computer setup --config {alps_files}alps_setup.yml"
    alps_config_command = f"verdi computer configure core.ssh daint.alps --config {alps_files}alps_config.yml"
    local_commands=[relabel_command,hide_code_command,alps_setup_command,alps_config_command]
    for command in local_commands:
        run_command(command)    

# setup codes

def setup_codes(list_of_codes):
    uenvs=[]
    for code_file in list_of_codes:
        # Extract label from YAML file
        code_data = load_yaml(alps_files +code_file)
        label = code_data.get("label")

        if not label:
            print(f"⚠️ Skipping {code_file}: No 'label' found in YAML.")
            continue
        else:
            prepend_text = code_data.get("prepend_text", "")  # Get the `prepend_text` field

            # Use regex to find the value of `--uenv=`
            match = re.search(r"#SBATCH --uenv=([\w\-/.:]+)", prepend_text)

            if match:
                uenv_value = match.group(1)  # Extract matched value
                print(f"File '{code_file}' → uenv: {uenv_value}")
                if uenv_value not in uenvs:
                    uenvs.append(uenv_value)
            else:
                print(f"⚠️ No '--uenv=' found in {code_file}")

        full_label = f"{label}@{new_host_label}"
        exported_file = "exported.yml"

        # Check if the code exists
        check_command = f"verdi code list | grep -q '{full_label}'"
        command_out,command_ok = run_command(check_command, ssh=False)
        if not command_ok:
            print(f"🔹 Code '{full_label}' not found. Installing...")
            install_command = f"verdi code create core.code.installed --config {alps_files}{code_file}"
            run_command(install_command, ssh=False)
        else:
            print(f"✅ Code '{full_label}' exists. Checking configuration...")

            # Export existing configuration
            export_command = f"verdi code export {full_label} {exported_file}"
            run_command(export_command, ssh=False)

            # Compare exported config with original YAML
            if load_yaml(exported_file) != code_data:
                print(f"🔄 Configuration differs for '{full_label}', relabeling and reinstalling...")

                # Relabel the existing code
                relabel_command = f"verdi code relabel {full_label} {relabeled_code}{full_label.split('@')[0]} && verdi code hide {relabeled_code}{full_label}"
                run_command(relabel_command, ssh=False)

                # Install the new code
                install_command = f"verdi code create core.code.installed --config {alps_files}{code_file}"
                run_command(install_command, ssh=False)
            else:
                print(f"✅ Configuration matches for '{full_label}', no changes needed.")
            run_command(f"rm -rf {exported_file}",ssh=False)
    print(f"Unevs needed: {uenvs}")
    return uenvs

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

# Manage uenvs

def manage_uenv_images(remote_host, uenvs):
    """
    Ensure that required uenv images are available on a remote host.
    
    :param remote_host: The remote machine where commands will be executed.
    :param uenvs: A list of required uenv images (e.g., ['cp2k/2024.3:v2', 'qe/7.4:v2'])
    """

    def extract_first_column(command_output):
        """Extracts only the first column (UENV image names) from multi-column output."""
        lines = command_output.split("\n")[1:]  # Skip the header line
        return {line.split()[0] for line in lines if line.strip()}  # Get first column values

    # Step 1: Check if the uenv repo exists, if not, create it
    print("🔍 Checking UENV repository status...")
    repo_status, command_ok = run_command("uenv repo status",ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to check UENV repo status. Exiting.")
        return False
    
    if "not found" in repo_status.lower() or not repo_status or "no repository" in repo_status.lower() :
        print("⚠️ UENV repo not found. Creating repository...")
        command_out,command_ok = run_command("uenv repo create",ssh=True,remotehost=remotehost)
        if not command_ok:
            print("❌ Failed to create UENV repo. Exiting.")
            return False
    else:
        print("✅ UENV repo is available.")

    # Step 2: Get the list of images available to the user
    print("🔍 Fetching available UENV images (user)...")
    available_user_images = extract_first_column(run_command("uenv image ls",ssh=True,remotehost=remotehost)[0])

    # Step 3: Get the list of all images available on the system
    print("🔍 Fetching available UENV images (system-wide)...")
    command_out,command_ok = run_command("uenv image ls ",ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to fetch system-wide UENV images. Exiting.")
        return False
    available_host_images = extract_first_column(command_out)
    command_out,command_ok = run_command("uenv image find service::",ssh=True,remotehost=remotehost)
    if not command_ok:
        print("❌ Failed to fetch service UENV images. Exiting.")
        return False    
    available_service_images = extract_first_column(command_out)

    # Step 4: Check missing images and pull them if necessary
    for uenv in uenvs:
        if uenv in available_user_images:
            print(f"✅ Image '{uenv}' is already available for the user.")
        elif uenv in available_host_images:
            print(f"✅ Image '{uenv}' is available on the host. Pulling...")
            run_command(f"uenv image pull {uenv}",ssh=True,remotehost=remotehost)
        elif uenv in available_service_images:
            print(f"✅ Image '{uenv}' is available in the service repo. Pulling from service::...")
            run_command(f"uenv image pull service::{uenv}",ssh=True,remotehost=remotehost)
        else:
            print(f"❌ Image '{uenv}' is not available anywhere! Manual intervention needed.")
            return False

    print("✅ UENV management complete.")
    return True

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
        return "<h2 style='color: green;'>✅ No old unfinished WorkChainNodes found.</h2>"
    
    msg = "<h2 style='color: darkorange;'>⚠️ Found old unfinished WorkChains</h2>"
    msg += "<p>Ask for help if you are unsure about removing them.</p><ul>"
    
    for pk in old_unfinished:
        if safe_to_delete(pk):
            msg += f"<li style='color: green;'>✅ WorkChain <strong>PK {pk}</strong> can be safely removed.</li>"
        else:
            msg += f"<li style='color: red;'>❌ WorkChain <strong>PK {pk}</strong> cannot be safely removed.</li>"
    
    msg += "</ul>"
    return msg