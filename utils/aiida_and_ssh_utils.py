from .string_utils import  remove_placeholders, normalize_text, relabel,to_camel_case
import subprocess
import yaml
import shutil
import time

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
        return f"❌ Computer '{computer_name}' not found in config.yml!<br>"

    # Step 2: Export the current setup and config from AiiDA
    setup_export_file = "setup.yml"
    config_export_file = "config.yml"

    try:
        subprocess.run(["verdi", "computer", "export", "setup", computer_name, setup_export_file], 
                       capture_output=True, text=True, check=True)
        subprocess.run(["verdi", "computer", "export", "config", computer_name, config_export_file],
                       capture_output=True, text=True, check=True,)
    except subprocess.CalledProcessError as e:
        return False, f"❌ Error exporting AiiDA computer setup/config: {e.stderr}<br>"

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
            return False,f"⚠️ **Setup Differences:**<br>"
    
    for entry in stored_config.keys(): 
        str1,str2 = remove_placeholders(normalize_text(str(stored_config[entry])), 
                                       normalize_text(str(exported_config[entry])))        
        if not str1==str2:
            return False,f"⚠️ **Config Differences:**<br>"        


    return True, "✅ No differences found! The stored configuration matches AiiDA.<br>"


def compare_code_configuration(stored_code_data):
    computer = stored_code_data['computer']
    code_label = stored_code_data['label']
    try:
        subprocess.run(["verdi", "code", "export", f"{code_label}@{computer}", "export.yml"],
                       capture_output=True, text=True,  check=True)
    except subprocess.CalledProcessError as e:
        return False, f"❌ Error exporting AiiDA code setup: {e.stderr}<br>"

    # Step 3: Load exported YAML files
    with open("export.yml", "r") as file:
        exported_setup = yaml.safe_load(file)

    #setup differences
    for entry in stored_code_data.keys():  
        str1,str2 = remove_placeholders(normalize_text(str(stored_code_data[entry])), 
                                       normalize_text(str(exported_setup[entry])))
        if not str1 == str2:
            return False,f"⚠️ **Setup Differences:**<br>"
        
    return True, f"✅ No differences found! The stored configuration for {code_label} matches AiiDA.<br>"

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

    result_msg += f"✅ Active AiiDA computers: {', '.join(active_computers)}<br>"
    result_msg += f"⬜ Not active AiiDA computers: {', '.join(not_active_computers)}<br>"
    return True,result_msg,active_computers,not_active_computers

def aiida_codes():
    result_msg = ""
    all_codes = set()
    codes = set()

    try:
        result = subprocess.run(["verdi", "code", "list", "-a"], capture_output=True, text=True, check=True)
        lines = result.stdout.splitlines()
        # [code,pk]
        all_codes = {(line.split()[0],line.split()[1]) for line in lines if "@" in line}
    except subprocess.CalledProcessError as e:        
        return False, f"❌ Error running 'verdi code list -a': {e.stderr}",set(),set()
    
    
    try:
        result = subprocess.run(["verdi", "code", "list"], capture_output=True, text=True, check=True)
        lines = result.stdout.splitlines()
        codes = {(line.split()[0],line.split()[1]) for line in lines if "@" in line}
    except subprocess.CalledProcessError as e:        
        return False, f"❌ Error running 'verdi code list': {e.stderr}",set(),set() 
    
    not_active_codes = all_codes - codes
    codes = codes
        
    result_msg += f"✅ Active AiiDA codes:<br> {'<br>'.join([code[0] + ' PK: ' + str(code[1]) for code in codes])}"

    result_msg += f"⬜ Not active AiiDA codes:<br> {'<br>'.join([code[0] + ' PK: ' + str(code[1]) for code in not_active_codes])}"
    return True,result_msg,codes,not_active_codes



def setup_aiida_computer(computer_name, config,hideold=False):
    """
    Sets up an AiiDA computer using `verdi computer setup` and configures SSH using `verdi computer configure core.ssh`.

    :param computer_name: The name of the computer (key from the config dictionary).
    :param config: The dictionary containing the setup and config details from the YAML file.
    """       
    setup = config[computer_name]["setup"]
    ssh_config = config[computer_name]["config"]

    if hideold:
        relabeled = relabel(computer_name)
        hide_comand = ["verdi","computer","relabel",setup["label"],relabeled]
        disable_command = ["verdi","computer","disable",relabeled,"aiida@localhost"]
    # Run computer setup
        for command in [hide_comand,disable_command]:
            try:
                subprocess.run(command, capture_output=True, text=True, check=True)
            except subprocess.CalledProcessError as e:
                print(f"❌ Error setting up computer '{computer_name}': {e}")
                return
        print(f"✅ Successfully relabeled computer '{computer_name}' to '{relabeled}'.")

    
    # Command for verdi computer setup
    setup_command = [
        "verdi", "computer", "setup",
        "--label", setup["label"],
        "--hostname", setup["hostname"],
        "--description", setup["description"],
        "--transport", setup["transport"],
        "--scheduler", setup["scheduler"],
        "--shebang", setup["shebang"],
        "--work-dir", setup["work_dir"],
        "--mpirun-command", setup["mpirun_command"],
        "--mpiprocs-per-machine", str(setup["mpiprocs_per_machine"]),
        "--default-memory-per-machine", str(setup["default_memory_per_machine"]),
        "--prepend-text", setup["prepend_text"]
    ]
    if setup["use_double_quotes"]:
        setup_command.append("--use-double-quotes")
    else:
        setup_command.append("--not-use-double-quotes")
    
    # Run computer setup
    try:
        subprocess.run(setup_command, capture_output=True, text=True, check=True)
        print(f"✅ Successfully set up computer '{computer_name}'.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error setting up computer '{computer_name}': {e}")
        return

    # Command for verdi computer configure core.ssh
    configure_command = [
        "verdi", "computer", "configure", setup["transport"], setup["label"],
        "--username", ssh_config["username"],
        "--port", str(ssh_config["port"]),
        "--look-for-keys" if ssh_config["look_for_keys"] else "--no-look-for-keys",
        "--key-filename", ssh_config["key_filename"],
        "--timeout", str(ssh_config["timeout"]),
        "--allow-agent" if ssh_config["allow_agent"] else "--no-allow-agent",
        "--proxy-jump", ssh_config["proxy_jump"] if ssh_config["proxy_jump"] else "",
        "--proxy-command", ssh_config["proxy_command"] if ssh_config["proxy_command"] else "",
        "--compress" if ssh_config["compress"] else "--no-compress",
        "--gss-auth", str(ssh_config["gss_auth"]),
        "--gss-kex", str(ssh_config["gss_kex"]),
        "--gss-deleg-creds", str(ssh_config["gss_deleg_creds"]),
        "--gss-host", ssh_config["gss_host"],
        "--load-system-host-keys" if ssh_config["load_system_host_keys"] else "--no-load-system-host-keys",
        "--key-policy", ssh_config["key_policy"],
        "--use-login-shell" if ssh_config["use_login_shell"] else "--no-use-login-shell",
        "--safe-interval", str(ssh_config["safe_interval"])
    ]
    
    # Remove empty options (proxy_jump and proxy_command if they are empty)
    configure_command = [arg for arg in configure_command if arg]

    # Run computer configuration
    try:
        subprocess.run(configure_command, capture_output=True, text=True, check=True)
        print(f"✅ Successfully configured SSH for computer '{computer_name}'.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error configuring SSH for computer '{computer_name}': {e}")


def setup_aiida_code(code_name, code_config,hide=False):
    """
    Sets up an AiiDA code using `verdi code create core.code.installed`.

    :param code_name: The name of the code (key from the config dictionary).
    :param config: The dictionary containing the setup details from the YAML file.
    """
    
    if hide:
        hide_command = ["verdi","code","hide",hide]
        try:
            subprocess.run(hide_command, check=True, capture_output=True, text=True)
            print(f"✅ Successfully hidded '{code_name}' {hide}.")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error hiding code '{code_name}' {hide}:\n{e.stderr}")        
        
    
    # Command for verdi code create core.code.installed
    code_command = [
        "verdi", "code", "create", "core.code.installed",
        "--computer", code_config["computer"],
        "--filepath-executable", code_config["filepath_executable"],
        "--label", code_config["label"],
        "--description", code_config["description"],
        "--default-calc-job-plugin", code_config["default_calc_job_plugin"],
        "--use-double-quotes" if code_config["use_double_quotes"] else "--no-use-double-quotes",
        "--with-mpi" if code_config["with_mpi"] else "--no-with-mpi",
        "--prepend-text", code_config.get("prepend_text", " "),
        "--append-text", code_config.get("append_text", " ")
    ]

    # Remove empty options (prepend/append text if not provided)
    code_command = [arg for arg in code_command if arg]

    # Run code setup
    try:
        subprocess.run(code_command, check=True, capture_output=True, text=True)
        print(f"✅ Successfully set up code '{code_name}'.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error setting up code '{code_name}':\n{e.stderr}")
        
def check_ssh_config(config_path, config_from_yaml):
    config_file = config_path / "config"
    msg=""
    reconfigure=[]
    all_up_to_date = True
    # Read the content of the config file
    try:
        with open(config_file, "r") as f:
            config_content = f.read()
            config_exist=True
    except FileNotFoundError:
        config_exist=False
        msg += f"Config file {config_file} not found. I will create it.<br>"
    
    for computer, details in config_from_yaml.items():
        setup = details.get("setup", {})
        config = details.get("config", {})

        hostname = setup.get("hostname")
        proxy_jump = config.get("proxy_jump", "")
        proxy_string = "Host " + proxy_jump if proxy_jump else ""

        # Check if hostname is in the config file
        need_to_update = True
        if config_exist:
            hostname_check = hostname in config_content if hostname else False
            proxy_check = proxy_string in config_content if proxy_jump else True  # Skip if empty
            need_to_update = not (hostname_check and proxy_check)

        if need_to_update:
            all_up_to_date = False
            if config_exist:
                msg+=f"⚠️{hostname} not properly configured in .ssh/config.<br>"
            reconfigure.append(computer)
    if msg == "":
        msg = "✅ The .ssh/config seems to be OK.<br>"
    
    return all_up_to_date, msg, reconfigure

def update_ssh_config(config_path,ssh_config_data,username,rename=True):
    
    # Ensure config_path exists
    config_path.mkdir(parents=True, exist_ok=True)

    # Define file paths
    config_file = config_path / "config"
    old_config_file = config_path / relabel("config") 
          
    if rename:
        shutil.move(config_file, old_config_file)
        print(f"✅ Renamed {config_file} → {old_config_file}")
        
    file_content = ""
    for host in ssh_config_data:
        ssh_config_data[host]['user'] = username
        file_content += f"Host {host}\n"
        for key, value in ssh_config_data[host].items():
            file_content += f"  {to_camel_case(key)} {value}\n"  # Capitalize the first letter of key
        file_content += "\n"


    with open(config_file, "w") as file:
        file.write(file_content + "\n")  # Ensure a newline at the end

    print(f"✅ Created new SSH config at {config_file}")
    return





import subprocess
import os

def set_ssh(config, hosts):
    """
    Adds SSH host keys to known_hosts for the specified hosts.

    Args:
        config (dict): SSH configuration details from YAML.
        hosts (list): List of hosts to update in known_hosts.

    Returns:
        bool: True if SSH check succeeds, False otherwise.
    """
    
    for computer in hosts:
        proxy = config[computer]["config"].get("proxy_jump", "")
        remotehost = config[computer]["setup"]["hostname"]

        if proxy:
            print(f"🔄 Adding {proxy} to known_hosts...")
            ssh_keyscan_command = ["ssh-keyscan", "-H", proxy]
            add_to_known_hosts(ssh_keyscan_command)

            print(f"🔄 Adding {remotehost} via {proxy} to known_hosts...")
            ssh_keyscan_command = ["ssh", proxy, "ssh-keyscan", "-H", remotehost]
            add_to_known_hosts(ssh_keyscan_command)
        else:
            print(f"🔄 Adding {remotehost} to known_hosts...")
            ssh_keyscan_command = ["ssh-keyscan", "-H", remotehost]
            add_to_known_hosts(ssh_keyscan_command)

    # Check if SSH works by listing the remote directory
    ssh_check_command = ["ssh", remotehost, "ls"]
    command_out, command_ok = run_command(ssh_check_command)

    return command_ok


def add_to_known_hosts(ssh_keyscan_command):
    """
    Runs ssh-keyscan and appends the output to ~/.ssh/known_hosts.

    Args:
        ssh_keyscan_command (list): The command to run (split properly).

    Returns:
        bool: True if the command succeeds, False otherwise.
    """
    try:
        with open(os.path.expanduser("~/.ssh/known_hosts"), "a") as f:
            known_host, success = run_command(ssh_keyscan_command)
            if success:
                f.write(known_host + "\n")
        return True  # Success

    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else "Unknown error (no stderr output)"
        print(f"❌ Error adding to known_hosts: {error_msg}")
    
    return False  # Failure



def run_command(command, max_retries=5):
    """
    Run a shell command locally or over SSH, capturing output and handling errors.
    If the error contains 'Connection closed by remote host', retries up to max_retries times with a 5-second wait.

    Args:
        command (list): The command to execute.
        max_retries (int): Maximum number of retries on connection failure.

    Returns:
        tuple: (command output as string, success as bool)
    """
    
    retries = max_retries if any(cmd in command for cmd in ["ssh", "scp", "ssh-keyscan"]) else 1
    attempts = 0

    while attempts < retries:
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(f"✅ Command executed successfully: {command}")
            return result.stdout.strip(), True
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip()
            print(f"❌ Error executing command:\n{error_msg}")

            if "Connection closed by remote host" in error_msg and attempts < retries - 1:
                attempts += 1
                print(f"🔄 Retrying in 5 seconds... (Attempt {attempts}/{retries})")
                time.sleep(5)
            else:
                return error_msg, False  # Return error message and success=False

    return "", False  # Should never reach this






# def set_ssh(config,hosts):

#     #command = f"ls /users/{cscs_username}"
#     #commnad_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
#     #if command_ok :
#     #    return True

#     for computer in hosts:
#         proxy = config[computer]["config"]["proxy_jump"]
#         remotehost = config[computer]["setup"]["hostname"]
#         if proxy:            
#             print(f"Adding {proxy} to known_hosts...")
#             ssh_keyscan_command = ["ssh-keyscan -H", proxy, ">> ~/.ssh/known_hosts"]
#             run_command(ssh_keyscan_command)

#             print(f"Adding {remotehost} to known_hosts...")
#             ssh_keyscan_command = ["ssh",proxy, "ssh-keyscan -H", remotehost, ">> ~/.ssh/known_hosts"]
#             run_command(ssh_keyscan_command)
#             #run_command(ssh_keyscan_command,ssh=False)
#         else:
#             print(f"Adding {remotehost} to known_hosts...")
#             ssh_keyscan_command = ["ssh-keyscan -H", remotehost, ">> ~/.ssh/known_hosts"]
#             run_command(ssh_keyscan_command)
#             #run_command(ssh_keyscan_command,ssh=False)

#     # check if ssh works
#     commnad_out,command_ok = run_command(command,ssh=True,remotehost=remotehost)
#     return command_ok

# def run_command(command, max_retries=5):
#     """
#     Run a shell command locally or over SSH, capturing output and handling errors.
#     If the error contains 'Connection closed by remote host', retries up to max_retries times with a 5-second wait.
    
#     Args:
#         command (list): The command to execute.
#         max_retries (int): Maximum number of retries on connection failure.
    
#     Returns:
#         tuple: (command output as string, success as bool)
#     """
    
#     # Set retries only for SSH-related commands
#     if any(cmd in command for cmd in ["ssh", "scp", "ssh-keyscan"]):
#         retries = max_retries
#     else:
#         retries = 1

#     attempts = 0
    
#     while attempts < retries:
#         try:
#             result = subprocess.run(command, check=True, capture_output=True, text=True)
#             return result.stdout.strip(), True  # Return output and success=True
#         except subprocess.CalledProcessError as e:
#             error_msg = e.stderr.strip()
#             print(f"❌ Error executing command:\n{error_msg}")

#             # Check for specific SSH error and retry
#             if "Connection closed by remote host" in error_msg and attempts < retries - 1:
#                 attempts += 1
#                 print(f"🔄 Retrying in 5 seconds... (Attempt {attempts}/{retries})")
#                 time.sleep(5)
#             else:
#                 return error_msg, False  # Return error message and success=False

#     return "", False  # Should never reach this, but ensures a valid return