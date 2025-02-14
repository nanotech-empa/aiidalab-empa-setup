import os
import re
import subprocess
from datetime import datetime
import yaml
import ipywidgets as ipw
from IPython.display import display
import time
python_version = '3.9.13'
new_host_label = 'daint.alps'
remotehost = "daint.alps.cscs.ch"  # Ensure this is the correct hostname
proxy = "ela.cscs.ch"
# labels for paths
repo_url = "https://github.com/nanotech-empa/aiidalab-alps-files.git"  # files needed on daint
repo_name = "aiidalab-alps-files"
home_dir = "/home/jovyan/"
alps_files = f"{home_dir}{repo_name}/"
config_path = f"{home_dir}.ssh/config"
config_source = f"{alps_files}config"
config_without_ela = f"{alps_files}config_without_ela"
# labels to rename old host and codes
relabeled_host = datetime.now().strftime("Used_till_%Y%m%d%H%M")+'_'+new_host_label
relabeled_code = datetime.now().strftime("Used_till_%Y%m%d%H%M")+'_'
yml_and_config_files = [
    "alps_setup.yml",
    "alps_config.yml",
    "cp2k.yml",
    "phonopy.yml",
    "python.yml",
    "critic2.yml",
    "config",
    "config_without_ela",
    'bashrc_template'
]
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


yml_files_to_reset = [
    'cp2k.yml',
    "phonopy.yml",
    "python.yml",
    "critic2.yml",
    'pw.yml',
    'pp.yml',
    'dos.yml',
    'projwfc.yml',
    'stm.yml',
    'overlap.yml',    
]
#process_yml_files(yml_files_to_reset)

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
def clone_repo(home_dir,repo_name):
    mydir = "/home/jovyan/"  # Directory containing the repo
    repo_path = os.path.join(home_dir, repo_name)

    if not os.path.isdir(repo_path):  # Check if the directory exists
        print(f"Cloning {repo_url} into {repo_path}...")
        commnad_out,command_ok = run_command(f"cd {home_dir} && git clone {repo_url}", ssh=False)
        if not command_ok:
            return
    else:
        print(f"Repository {repo_name} exists, pulling latest changes...")
        commnad_out,command_ok = run_command(f"cd {repo_path} && git reset --hard HEAD && git pull", ssh=False)
        if not command_ok:
            return
    return

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

# Example usage

# Define computer label and file paths
config_file = "config.yml"
setup_file = "setup.yml"
ref_config = alps_files + "alps_config.yml"
ref_setup = alps_files + "alps_setup.yml"

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


