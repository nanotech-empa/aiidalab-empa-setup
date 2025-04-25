from .string_utils import   normalize_text, relabel,to_camel_case #remove_placeholders
from datetime import datetime,timedelta
import subprocess
import yaml
import shutil
import time
import os
import re
from aiida.orm import QueryBuilder, WorkChainNode, CalcJobNode, StructureData, Node
from aiida import load_profile
from aiida.orm import load_node

def run_command(command, max_retries=5,verbose=False):
    """
    Run a shell command locally or over SSH, capturing output and handling errors.
    Retries on 'Connection closed by remote host' errors.
    """
    retries = max_retries if any(cmd in command for cmd in ["ssh", "scp", "ssh-keyscan"]) else 1
    attempts = 0

    while attempts < retries:
        output, success = "", False
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            output, success = result.stdout.strip(), True
            if verbose:
                print(f"✅ Command executed successfully: {command}")
            return output, success
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip()
            if verbose:
                print(f"❌ Error executing command: {error_msg}")
            
            if "Connection closed by remote host" in error_msg and attempts < retries - 1:
                attempts += 1
                if(verbose):
                    print(f"🔄 Retrying in 5 seconds... (Attempt {attempts}/{retries})")
                time.sleep(5)
            else:
                return error_msg, False  # Return error message and success=False

    return "", False  # Should never reach this

def compare_computer_configuration(computer_name, repository_computer_data):
    """
    Compares the setup and config of a computer in AiiDA against stored values.
    """
    repository_setup = repository_computer_data.get("setup", {})
    repository_config = repository_computer_data.get("config", {})
    
    if not repository_setup or not repository_config:
        return False, f"❌ Computer '{computer_name}' not found in config.yml!<br>"

    setup_export_file, config_export_file = "setup.yml", "config.yml"
    commands = [
        ["verdi", "computer", "export", "setup", computer_name, setup_export_file],
        ["verdi", "computer", "export", "config", computer_name, config_export_file]
    ]
    
    for cmd in commands:
        output, success = run_command(cmd)
        if not success:
            return False, f"❌ Error exporting AiiDA computer setup/config: {output}<br>"

    with open(setup_export_file, "r") as file:
        exported_setup = yaml.safe_load(file)
    with open(config_export_file, "r") as file:
        exported_config = yaml.safe_load(file)

    for entry in repository_setup:
        #str1, str2 = remove_placeholders(normalize_text(str(repository_setup[entry])), normalize_text(str(exported_setup.get(entry, ""))))
        str1 = normalize_text(str(repository_setup[entry]))
        str2 = normalize_text(str(exported_setup.get(entry, "")))
        if str1 != str2:
            return False, f"⚠️ **Setup Differences:** {entry}<br>"

    for entry in repository_config:
        #str1, str2 = remove_placeholders(normalize_text(str(repository_config[entry])), normalize_text(str(exported_config.get(entry, ""))))
        str1 = normalize_text(str(repository_config[entry]))
        str2 = normalize_text(str(exported_config.get(entry, "")))
        if str1 != str2:
            return False, f"⚠️ **Config Differences:** {entry}<br>"

    return True, "✅ No differences found! The stored configuration matches AiiDA.<br>"

def compare_code_configuration(code_label, repository_code_data):
    """
    Compares the setup of an AiiDA code against stored values.
    """
    export_file = "export.yml"
    
    output, success = run_command(["verdi", "code", "export", code_label, export_file])
    if not success:
        return False, f"❌ Error exporting AiiDA code setup: {output}<br>"

    with open(export_file, "r") as file:
        exported_setup = yaml.safe_load(file)

    for entry in repository_code_data:
        
        #str1, str2 = remove_placeholders(normalize_text(str(repository_code_data[entry])), normalize_text(str(exported_setup.get(entry, ""))))
        str1 = normalize_text(str(repository_code_data[entry]))
        str2 = normalize_text(str(exported_setup.get(entry, "")))
        if entry == 'computer':
            str2 = str2.split('_', 1)[0]
        if str1 != str2:
            return False, f"⚠️ **Setup Differences:** {entry}<br>"
    
    return True, f"✅ No differences found! The stored configuration for {code_label} matches AiiDA.<br>"

def aiida_computers():
    result_msg = ""
    active_computers = set()
    not_active_computers = set()

    output, success = run_command(["verdi", "computer", "list", "-a"])
    if not success:
        return False, f"❌ Error running 'verdi computer list -a': {output}", active_computers, not_active_computers

    for line in output.splitlines():
        stripped_line = line.strip()
        if stripped_line.startswith("* "):
            active_computers.add(stripped_line[2:])
        elif stripped_line and not stripped_line.startswith("Report:"):
            not_active_computers.add(stripped_line)

    result_msg += f"✅ Active AiiDA computers: {'<br>'.join([f'✅{comp}' for comp in active_computers])}"
    result_msg += "<br>"
    result_msg += f"✅⬜ Not active AiiDA computers: {'<br>'.join([f'✅⬜{comp}' for comp in not_active_computers])}"
    result_msg += "<br>"

    return True, result_msg, active_computers, not_active_computers

def aiida_codes():
    result_msg = ""
    all_codes = set()
    codes = set()

    output, success = run_command(["verdi", "code", "list", "-a"])
    if not success:
        return False, f"❌ Error running 'verdi code list -a': {output}<br>", set(), set()
    all_codes = {(line.split()[0], line.split()[1]) for line in output.splitlines() if "@" in line}
    
    output, success = run_command(["verdi", "code", "list"])
    if not success:
        return False, f"❌ Error running 'verdi code list': {output}<br>", set(), set()
    codes = {(line.split()[0], line.split()[1]) for line in output.splitlines() if "@" in line}

    not_active_codes = all_codes - codes
    active_section = ('<br>'.join([f"✅ {code[0]}  PK: {str(code[1])}" for code in (codes or [])]) if codes else "None")
    not_active_section = ('<br>'.join([f"✅⬜{code[0]} PK: {str(code[1])}" for code in (not_active_codes or [])]) if not_active_codes else "None")

    result_msg += f"✅ Active AiiDA codes:<br> {active_section}"
    result_msg += "<br>"
    result_msg += f"✅⬜ Not active AiiDA codes:<br> {not_active_section}"
    result_msg += "<br>"
    return True, result_msg, codes, not_active_codes


def setup_aiida_computer(computer_name, config, hide=False, torelabel=False, install=False, grant=''):
    """
    Sets up an AiiDA computer using `verdi computer setup` and configures SSH.
    """

    relabeled = relabel(computer_name) if torelabel else computer_name
    commands = [["verdi", "computer", "relabel", computer_name, relabeled]] if torelabel else []    
    if hide:
        commands.append(["verdi", "computer", "disable", relabeled, "aiida@localhost"])
        
    for command in commands:
        output, success = run_command(command)
        if not success:
            print(f"❌ Error relabelling/deactivating '{computer_name}': {output}")
            return False
    print(f"✅ Successfully relabeled/hidden computer '{computer_name}' to '{relabeled}'.")

    if install:
        setup = config["setup"]
        ssh_config = config["config"]
        setup_command = [
            "verdi", "computer", "setup",
            "--label", computer_name,
            "--hostname", setup["hostname"],
            "--description", setup["description"],
            "--transport", setup["transport"],
            "--scheduler", setup["scheduler"],
            "--shebang", setup["shebang"],
            "--work-dir", setup["work_dir"],
            "--mpirun-command", setup["mpirun_command"],
            "--mpiprocs-per-machine", str(setup["mpiprocs_per_machine"]),
            "--default-memory-per-machine", str(setup["default_memory_per_machine"]),
            "--prepend-text", setup["prepend_text"].replace('cscsaccount', grant),
            "--non-interactive",
        ] + (["--use-double-quotes"] if setup["use_double_quotes"] else ["--not-use-double-quotes"])
        
        output, success = run_command(setup_command)
        if not success:
            print(f"❌ Error setting up computer '{computer_name}': {output}")
            return False
        print(f"✅ Successfully set up computer '{computer_name}'.")
        
        configure_command = [
            "verdi", "computer", "configure", setup["transport"], computer_name,
            "--username", ssh_config["username"],
            "--port", str(ssh_config["port"]),
            "--look-for-keys" if ssh_config["look_for_keys"] else "--no-look-for-keys",
            "--key-filename", ssh_config["key_filename"],
            "--timeout", str(ssh_config["timeout"]),
            "--allow-agent" if ssh_config["allow_agent"] else "--no-allow-agent",
            "--compress" if ssh_config["compress"] else "--no-compress",
            "--gss-auth", str(ssh_config["gss_auth"]),
            "--gss-kex", str(ssh_config["gss_kex"]),
            "--gss-deleg-creds", str(ssh_config["gss_deleg_creds"]),
            "--gss-host", ssh_config["gss_host"],
            "--load-system-host-keys" if ssh_config["load_system_host_keys"] else "--no-load-system-host-keys",
            "--key-policy", ssh_config["key_policy"],
            "--use-login-shell" if ssh_config["use_login_shell"] else "--no-use-login-shell",
            "--safe-interval", str(ssh_config["safe_interval"]),
            "--non-interactive",
        ]

        # Conditionally append --proxy-jump if not empty
        if ssh_config.get("proxy_jump"):
            configure_command.extend(["--proxy-jump", ssh_config["proxy_jump"]])

        # Conditionally append --proxy-command if not empty
        if ssh_config.get("proxy_command"):
            configure_command.extend(["--proxy-command", ssh_config["proxy_command"]])

    
        output, success = run_command(configure_command)
        if not success:
            print(f"❌ Error configuring SSH for computer '{computer_name}': {output}")
            return False
        print(f"✅ Successfully configured SSH for computer '{computer_name}'.")        
    
        
    return True

def setup_aiida_code(code_name, code_config, hide=False, pktorelabel=False, install=False):
    """
    Sets up an AiiDA code using `verdi code create core.code.installed`.
    """
    # code_name pw-7.4:v2@daint.alps_s1267
    code = code_name.split("@")[0]
    computer = code_name.split("@")[1]
    relabeled = relabel(code) if pktorelabel else code
    if pktorelabel:
        output, success = run_command(["verdi", "code", "relabel", str(pktorelabel), relabeled])
        if not success:
            print(f"❌ Error relabelling '{code_name}': {output}")
            return False
    
    if hide:
        output, success = run_command(["verdi", "code", "hide", str(pktorelabel)])
        if not success:
            print(f"❌ Error hiding code '{code_name}': {output}")
            return False
    
    if install:
        code_command = [
            "verdi", "code", "create", "core.code.installed",
            "--computer", computer, 
            "--filepath-executable", code_config["filepath_executable"],
            "--label", relabeled,
            "--description", code_config["description"],
            "--default-calc-job-plugin", code_config["default_calc_job_plugin"],
            "--prepend-text", code_config.get("prepend_text", " "),
            "--append-text", code_config.get("append_text", " ")
        ] + (["--use-double-quotes"] if code_config.get("use_double_quotes", False) else ["--no-use-double-quotes"])
        
        output, success = run_command(code_command)
        if not success:
            print(f"❌ Error setting up code '{code_name}': {output}")
            return False
        print(f"✅ Successfully set up code '{code_name}'.")
    return True
        
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

def update_ssh_config(config_path,ssh_config_data,rename=True):
    
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
        #ssh_config_data[host]['user'] = username
        file_content += f"Host {host}\n"
        for key, value in ssh_config_data[host].items():
            file_content += f"  {to_camel_case(key)} {value}\n"  # Capitalize the first letter of key
        file_content += "\n"


    with open(config_file, "w") as file:
        file.write(file_content + "\n")  # Ensure a newline at the end

    print(f"✅ Created new SSH config at {config_file}")
    return

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

def execute_custom_commands(yaml_commands):
    """Execute all commands from custom_commands in the YAML file."""    
    if "custom_commands" not in yaml_commands:
        print("❌ No custom commands found in YAML file. Exiting.")
        return False
    
    # Execute remote computer commands
    remote_commands = yaml_commands["custom_commands"].get("remote_commands", {})
    remotehost = remote_commands.pop('remotehost') # remove the remotehost from the dictionary after assigning it
    for setup_name, commands in remote_commands.items():
        print(f"🔄 Executing remote commands for {setup_name} on {remotehost}...")
        for entry in commands:
            formatted_command = entry["command"]
            remote_command = ["ssh", remotehost, formatted_command] if entry["type"] == "ssh" else formatted_command.split()
            output, success = run_command(remote_command)
            if not success:
                print(f"❌ Failed to execute: {entry['type']} {formatted_command}. Exiting, ask for help.")
                return False
    return True
    
def parse_validity_time(public_key_file):
    """Parse the validity time from the output."""
    output = subprocess.run(
        ["ssh-keygen", "-L", "-f", public_key_file],
        encoding="utf-8",
        capture_output=True,
    ).stdout

    matched_line = (
        re.search(r"^.*{}.*$".format("Valid:"), output, flags=re.MULTILINE)
        .group(0)
        .split()
    )
    start = datetime.fromisoformat(matched_line[2])
    end = datetime.fromisoformat(matched_line[4])
    return start, end

def key_is_valid(public_key_file = ''):
    """Check if the key is valid."""
    start, end = parse_validity_time(public_key_file)
    if start < datetime.now() < end:
        return True
    else:
        return False
    
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

def get_old_unfinished_workchains(cutoffdays=30,reverse=False,paused=False):
    """
    Returns a formatted message with all WorkChainNodes that are older than 30 days and unfinished.
    
    :return: HTML formatted message with green (✅) and red (❌) indicators.
    """
    nodes = [WorkChainNode]
    project = ['id']
    if not load_profile():
        load_profile("default")
    cutoff_date = datetime.now() - timedelta(days=cutoffdays)
    filters = {
            'ctime': {'<': cutoff_date},  # Created more than x days ago
            'attributes.process_state': {'!in': ['finished', 'excepted', 'killed']}  # Not finished
        }
    if reverse:
        filters = {
            'ctime': {'>': cutoff_date},  # Created less than x days ago
            'attributes.process_state': {'!in': ['finished', 'excepted','killed']}  # Running or waiting
        }
    if paused:     
        nodes = [WorkChainNode, CalcJobNode]  
        project = ['id','attributes.paused'] 
        filters = {
            'ctime': {'>': cutoff_date},  # Created less than x days ago
            'attributes.process_state': {'!in': ['finished', 'excepted','killed']},# Running or waiting
            'attributes.paused': True  # Paused
        }
    qb = QueryBuilder()
    qb.append(
        nodes, 
        filters=filters,
        project=project  # Retrieve PKs only
    )
    
    old_unfinished = [entry[0] for entry in qb.all()]
    if not old_unfinished:
        return False,"<style='color: green;'>✅ No old unfinished WorkChainNodes found.<br>"
    
    if paused:
        msg = ' '.join(str(num) for num in old_unfinished)
        return True,msg
    msg = "<style='color: darkorange;'>⚠️ Found old unfinished WorkChains<br>"
    msg += "<p>Ask for help if you are unsure about removing them.</p><ul>"
    
    for pk in old_unfinished:
        if safe_to_delete(pk):
            msg += f"<li style='color: green;'>✅ WorkChain <strong>PK {pk}</strong> can be safely removed.</li>"
        else:
            msg += f"<li style='color: red;'>❌ WorkChain <strong>PK {pk}</strong> cannot be safely removed.</li>"
    
    msg += "</ul>"
    return True,msg
def play_paused_workchains(paused_workchains):
    """
    Replays paused workchains.
    
    :param paused_workchains: string of paused workchain PKs.
    """
    if not paused_workchains:
        return "No paused workchains to play.",True
    return run_command(["verdi", "process", "play"]+ paused_workchains.split())