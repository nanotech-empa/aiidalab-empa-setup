import os
import re
from datetime import datetime
from pathlib import Path

import yaml

from .aiida_and_ssh_utils import (
    aiida_codes,
    aiida_computers,
    check_ssh_config,
    compare_code_configuration,
    compare_computer_configuration,
    run_command,
    setup_aiida_code,
    setup_aiida_computer,
)
from .repo_utils import (
    BRANCH,
    GIT_REPO_PATH,
    clone_repository,
    config_path,
    get_latest_remote_commit,
    get_local_commit,
    pull_latest_changes,
    switch_config_branch,
)
from .string_utils import extract_first_column

SUPPORTED_SCHEMA_VERSION = 1


# Check repository of config files
def check_repository(branch=BRANCH):
    """Check if the repository exists and pull the latest changes."""
    msg = "<b style='color:green;'>✅ Repository is up to date.</b>"
    if not os.path.exists(GIT_REPO_PATH):
        msg = (
            "<b style='color:orange;'>⚠️ Repository updated. "
            "Please inspect and then apply changes.</b>"
        )
        if not clone_repository(branch=branch):
            return (
                False,
                "<b style='color:red;'>❌ Failed to clone the repository. "
                "Please check your configuration.</b>",
            )

    branch_ok, branch_msg = switch_config_branch(branch)
    if not branch_ok:
        return False, f"<b style='color:red;'>❌ {branch_msg}</b>"

    local_commit = get_local_commit()
    remote_commit = get_latest_remote_commit(branch=branch)

    if not local_commit:
        return False, "<b style='color:red;'>❌ Unable to check for updates.</b>"

    if not remote_commit:
        return (
            True,
            (
                "<b style='color:orange;'>⚠️ Using local config branch "
                f"'{branch}'. No matching remote branch was found.</b>"
            ),
        )

    if local_commit != remote_commit:
        if not pull_latest_changes(branch=branch):
            return False, "<b style='color:red;'>❌ Failed to update the repository.</b>"
        msg = "<b style='color:orange;'>⚠️ Repository updated. Please apply changes.</b>"

    return True, msg


def get_config(
    file_path='/home/jovyan/opt/aiidalab-alps-files/config.yml',
    config_widgets=None,
    branch=BRANCH,
):
    """Get the configuration from the YAML file."""
    config_widgets = config_widgets or {}

    for key, widget in config_widgets.items():
        if widget.value == "select":
            return False, f"<b style='color:red;'>❌please select {key}</b>", {}

    status_ok, msg = check_repository(branch=branch)
    if not status_ok:
        return status_ok, msg, {}

    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)

    try:
        schema_version = int(data.get("schema_version", 1))
    except (TypeError, ValueError):
        return (
            False,
            "<b style='color:red;'>❌ Invalid config schema_version.</b>",
            {},
        )
    if schema_version > SUPPORTED_SCHEMA_VERSION:
        return (
            False,
            (
                "<b style='color:red;'>❌ This setup app supports config schema "
                f"{SUPPORTED_SCHEMA_VERSION}, but the YAML file uses schema "
                f"{schema_version}. Please update the setup app.</b>"
            ),
            {},
        )

    variables = data.get("variables", {})
    if 'timestamp' in variables and variables['timestamp'] == 'now':
        variables['timestamp'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    widgets = data.get("widgets", {})

    def replace_in_string(s, replacements):
        for key, value in replacements.items():
            s = s.replace(f"{{{key}}}", value)
        return s

    def recursive_replace(obj, replacements):
        if isinstance(obj, dict):
            return {k: recursive_replace(v, replacements) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [recursive_replace(item, replacements) for item in obj]
        elif isinstance(obj, str):
            return replace_in_string(obj, replacements)
        else:
            return obj

    widget_replacements = {
        key: widget.value for key, widget in config_widgets.items() if key in widgets
    }

    for key, value in variables.items():
        if isinstance(value, str):
            variables[key] = replace_in_string(value, widget_replacements)

    all_replacements = {}
    all_replacements.update(widget_replacements)
    all_replacements.update(
        {key: value for key, value in variables.items() if isinstance(value, str)}
    )

    data = recursive_replace(data, all_replacements)
    data["_metadata"] = {
        "schema_version": schema_version,
        "config_branch": branch,
        "config_revision": (get_local_commit() or "unknown")[:12],
    }

    return True, '', data


def _configured_computer_labels(defined_computers):
    """Return labels that may appear in AiiDA for all configured grants."""
    labels = []
    for computer_name, computer_data in defined_computers.items():
        label_template = computer_data.get("setup", {}).get("label", computer_name)
        for grant in computer_data.get("grants", []):
            if "{grant}" in label_template:
                labels.append(label_template.replace("{grant}", grant))
            else:
                labels.append(f"{computer_name}_{grant}")
    return labels


def _selected_computer_labels(defined_computers, selected_grant):
    """Return configured computer labels that match the selected grant."""
    return {
        computer_data.get("setup", {}).get("label", computer_name)
        for computer_name, computer_data in defined_computers.items()
        if selected_grant in computer_data.get("grants", [])
    }


def _computer_config_by_label(defined_computers):
    return {
        computer_data.get("setup", {}).get("label", computer_name): (
            computer_name,
            computer_data,
        )
        for computer_name, computer_data in defined_computers.items()
    }


def _queue_update(updates_needed, section, label, **update):
    updates_needed.setdefault(section, {})[label] = update


def check_for_updates(config, selected_grant):
    """Check the user's AiiDA setup against the selected YAML configuration."""
    status, msg, updates_needed = process_aiida_configuration(
        config, config_path, selected_grant
    )
    if not status:
        return msg, {}
    if not updates_needed:
        return "<b style='color:green;'>✅ Your configuration is up to date.</b>", {}
    return msg, updates_needed


def process_aiida_configuration(config, config_path, selected_grant):
    """
    Reads the YAML configuration file, renames the existing SSH config, 
    creates a new SSH config from the YAML file, and checks installed vs. missing AiiDA computers.
    
    :param configuration_file: Path to the YAML configuration file.
    :param config_path: Path to the SSH config directory.
    :return: Formatted string with the results.
    """
    updates_needed = {}
    config_path = Path(config_path)
    result_msg = ""

    config_ok, msg, config_hosts = check_ssh_config(config_path, config["computers"])
    result_msg += msg
    if not config_ok:
        updates_needed["ssh_config"] = {
            "rename": "not properly" in msg,
            "hosts": config_hosts,
        }

    status_computers, msg, active_computers, not_active_computers = aiida_computers()
    result_msg += msg
    status_codes, msg, active_codes, not_active_codes = aiida_codes()
    result_msg += msg
    if not (status_computers and status_codes):
        return False, result_msg + msg

    defined_computers = config.get("computers", {})
    valid_computer_grants = _configured_computer_labels(defined_computers)
    selected_computer_grants = _selected_computer_labels(
        defined_computers, selected_grant
    )

    valid_computer_grants += ["localhost"]

    for computer in active_computers:
        if computer not in valid_computer_grants:
            result_msg += (
                f"⚠️ Computer '{computer}' is installed in AiiDA but is not foreseen "
                "in the configuration file.<br>"
            )
            _queue_update(
                updates_needed,
                "computers",
                computer,
                hide=True,
                rename=False,
                install=False,
            )

    for comp_data in defined_computers.values():
        full_comp = comp_data["setup"]["label"]
        if full_comp in active_computers:
            result_msg += (
                f"✅⬜ Computer '{full_comp}' is already installed in AiiDA, "
                "checking for its configuration.<br>"
            )
            is_up_to_date, msg = compare_computer_configuration(full_comp, comp_data)
            result_msg += msg
            if not is_up_to_date:
                install = full_comp in selected_computer_grants
                _queue_update(
                    updates_needed,
                    "computers",
                    full_comp,
                    hide=True,
                    rename=True,
                    install=install,
                )

        elif full_comp in not_active_computers:
            result_msg += f"⬜ Computer '{full_comp}' is listed but NOT active in AiiDA.<br>"
            install = full_comp in selected_computer_grants
            _queue_update(
                updates_needed,
                "computers",
                full_comp,
                hide=False,
                rename=True,
                install=install,
            )

        else:
            install = full_comp in selected_computer_grants
            if install:
                result_msg += f"❌ Computer '{full_comp}' is completely missing from AiiDA.<br>"
                _queue_update(
                    updates_needed,
                    "computers",
                    full_comp,
                    hide=False,
                    rename=False,
                    install=install,
                )

    defined_codes = config.get("codes", {})
    for codename, codecomputer, code_pk in active_codes:
        code_label = f"{codename}@{codecomputer}"
        if codecomputer not in valid_computer_grants:
            result_msg += (
                f"⚠️ Code '{codename}' is installed in AiiDA but its computer/grant "
                "is not defined in the configuration file.<br>"
            )
            _queue_update(
                updates_needed,
                "codes",
                code_label,
                hide=code_pk,
                rename=code_pk,
                install=False,
            )

    for code_key, code_data in defined_codes.items():
        computer = defined_computers[code_data["computer"]]["setup"]["label"]
        install = computer in selected_computer_grants
        computer_update = updates_needed.get("computers", {}).get(computer, {})
        computer_will_be_outdated = bool(computer_update) and not computer_update.get(
            "install", False
        )
        computer_will_be_installed = bool(computer_update) and computer_update.get(
            "install", False
        )
        computer_up_to_date = computer in active_computers and not computer_will_be_installed
        code_label = f"{code_data['label']}@{computer}"
        code_pk_active = next(
            (
                pk
                for codename, codecomputer, pk in active_codes
                if f"{codename}@{codecomputer}" == code_label
            ),
            None,
        )
        code_pk_not_active = next(
            (
                pk
                for codename, codecomputer, pk in not_active_codes
                if f"{codename}@{codecomputer}" == code_label
            ),
            None,
        )

        msg = f"✅ Code {code_label} is already installed in AiiDA.<br>"

        if computer_will_be_outdated:
            if code_pk_active is not None:
                _queue_update(
                    updates_needed,
                    "codes",
                    code_label,
                    code_key=code_key,
                    rename=code_pk_active,
                    hide=True,
                    install=False,
                )
                msg = (
                    f"⚠️ Code {code_label} is installed in AiiDA on an old computer. "
                    "It will be renamed and reinstalled.<br>"
                )
            elif code_pk_not_active is not None:
                _queue_update(
                    updates_needed,
                    "codes",
                    code_label,
                    code_key=code_key,
                    rename=code_pk_not_active,
                    hide=False,
                    install=False,
                )
                msg = (
                    f"⚠️ Code {code_label} is installed but not active and points to "
                    "an old computer. It will be renamed and reinstalled.<br>"
                )
        elif computer_will_be_installed:
            if install:
                _queue_update(
                    updates_needed,
                    "codes",
                    code_label,
                    code_key=code_key,
                    rename=False,
                    install=True,
                )
                msg = f"⬜ Code {code_label} will be installed  {computer} will be installed.<br>"
        elif computer_up_to_date:
            if install:
                if code_pk_active is not None:
                    codes_equal, msg = compare_code_configuration(code_label, code_data)
                    if not codes_equal:
                        _queue_update(
                            updates_needed,
                            "codes",
                            code_label,
                            code_key=code_key,
                            rename=code_pk_active,
                            install=True,
                        )
                        msg = f"⬜ Code {code_label} will be installed  {computer} is present.<br>"
                    else:
                        _queue_update(
                            updates_needed,
                            "codes",
                            code_label,
                            code_key=code_key,
                            checkuenv=True,
                            install=False,
                        )
                        msg = (
                            f"✅ Code {code_label} is already installed and up-to-date; "
                            "uenv availability will be checked.<br>"
                        )
                elif code_pk_not_active is not None:
                    _queue_update(
                        updates_needed,
                        "codes",
                        code_label,
                        code_key=code_key,
                        rename=code_pk_not_active,
                        install=True,
                    )
                    msg = (
                        f"⬜ Code {code_label} will be installed; the old inactive "
                        "code will be renamed.<br>"
                    )
                else:
                    _queue_update(
                        updates_needed,
                        "codes",
                        code_label,
                        code_key=code_key,
                        rename=False,
                        install=True,
                    )
                    msg = f"⬜ Code {code_label} will be installed  {computer} is present.<br>"

        result_msg += msg

    return True, result_msg, updates_needed


def setup_computers(computers_to_setup, defined_computers):
    computer_config_by_label = _computer_config_by_label(defined_computers)

    for computer, update in computers_to_setup.items():
        _, config_computers = computer_config_by_label.get(
            computer, (computer.rsplit("_", 1)[0], {})
        )
        _, _, grant = computer.rpartition("_")
        print(f"🔄 Dealing with '{computer}'")
        status = setup_aiida_computer(
            computer,
            config_computers,
            hide=update.get("hide", False),
            torelabel=update.get("rename", False),
            install=update.get("install", False),
            grant=grant,
        )
        if not status:
            return False

    return True


def setup_codes(codes_to_setup, config):
    defined_codes = config.get("codes", {})
    uenvs = []
    for full_code, update in codes_to_setup.items():
        hide = update.get("hide", False)
        pktorelabel = update.get("rename", False)
        install = update.get("install", False)
        checkuenv = update.get("checkuenv", False)
        code = update.get("code_key")
        code_data = {}

        if install or checkuenv:
            if code not in defined_codes:
                print(f"❌ Code '{full_code}' is missing from the YAML definition.")
                return False, uenvs
            code_data = defined_codes[code]
            computer = code_data["computer"]
            hostname = config["computers"][computer]["setup"]["hostname"]
            prepend_text = code_data.get("prepend_text", "")
            match = re.search(r"#SBATCH --uenv=([\w\-/.:]+)", prepend_text)
            if match:
                uenv_value = match.group(1)
                print(f"⬜  Need uenv: {uenv_value} for '{full_code}'")
                if (hostname, uenv_value) not in uenvs:
                    uenvs.append((hostname, uenv_value))
            else:
                print(f"✅ No uenv needed for '{full_code}'")

        status = setup_aiida_code(
            full_code,
            code_data,
            hide=hide,
            pktorelabel=pktorelabel,
            install=install,
        )
        if not status:
            return False, uenvs

    return True, uenvs


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
        
        if (
            "not found" in repo_status.lower()
            or not repo_status
            or "no repository" in repo_status.lower()
        ):
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
            if not command_ok:
                print(f"❌ Failed to pull image '{env}' on {remotehost}: {command_out}")
                return False
        elif env in available_images[remotehost]['service']:
            print(
                f"✅ Image '{env}' is available in the service repo on {remotehost}. "
                "Pulling from service::..."
            )
            command = ["ssh", remotehost, "uenv", "image", "pull", f"service::{env}"]
            command_out, command_ok = run_command(command)
            if not command_ok:
                print(f"❌ Failed to pull service image '{env}' on {remotehost}: {command_out}")
                return False
        else:
            print(
                f"❌ Image '{env}' is not available anywhere on {remotehost}! "
                "Manual intervention needed."
            )
            return False

    print("✅ UENV management complete.")
    return True
