import os
import re
from datetime import datetime
from html import escape
from importlib.metadata import entry_points
from pathlib import Path

import yaml

from .aiida_and_ssh_utils import (
    aiida_codes,
    aiida_computers,
    check_ssh_config,
    compare_code_configuration,
    compare_computer_configuration,
    setup_aiida_code,
    setup_aiida_computer,
)
from .configuration import load_config_bundle
from .readiness import (
    manage_uenv_images as manage_uenv_images,  # public compatibility import
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
from .selection import installation_tasks, select_resources

SUPPORTED_SCHEMA_VERSION = 2


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
            return (
                False,
                "<b style='color:red;'>❌ Failed to update the repository.</b>",
            )
        msg = "<b style='color:orange;'>⚠️ Repository updated. Please apply changes.</b>"

    return True, msg


def get_config(
    file_path="/home/jovyan/opt/aiidalab-alps-files/config.yml",
    config_widgets=None,
    branch=BRANCH,
    deployment="hosted",
    computer_grants=None,
    settings=None,
):
    """Get the configuration from the YAML file."""
    config_widgets = config_widgets or {}

    for key, widget in config_widgets.items():
        if widget.value == "select" and not (
            key == "cscsuser" and deployment == "personal" and not computer_grants
        ):
            return False, f"<b style='color:red;'>❌please select {key}</b>", {}

    status_ok, msg = check_repository(branch=branch)
    if not status_ok:
        return status_ok, msg, {}

    try:
        data = load_config_bundle(file_path)
    except (OSError, ValueError, yaml.YAMLError) as error:
        return False, f"❌ Cannot read configuration: {escape(str(error))}", {}

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
    variables.update(settings or {})
    if "timestamp" in variables and variables["timestamp"] == "now":
        variables["timestamp"] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
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
    all_replacements.update(settings or {})
    all_replacements.update(
        {key: value for key, value in variables.items() if isinstance(value, str)}
    )

    # Resolve per-computer accounts before the global legacy grant placeholder.
    account_map = computer_grants or {}
    for key, computer in data.get("computers", {}).items():
        grant = account_map.get(key, widget_replacements.get("grant"))
        if grant is not None:
            data["computers"][key] = recursive_replace(computer, {"grant": grant})
    data = recursive_replace(data, all_replacements)
    data["_deployment"] = deployment
    data["_computer_grants"] = account_map
    data["_metadata"] = {
        "schema_version": schema_version,
        "config_branch": branch,
        "config_revision": get_local_commit() or "unknown",
    }

    return True, "", data


def _configured_computer_labels(defined_computers):
    """Return labels that may appear in AiiDA for all configured grants."""
    labels = []
    for computer_name, computer_data in defined_computers.items():
        template = computer_data.get("setup", {}).get("label", computer_name)
        grants = computer_data.get("grants", [])
        if not grants:
            labels.append(template)
        for grant in grants:
            labels.append(
                template.replace("{grant}", grant)
                if "{grant}" in template
                else f"{computer_name}_{grant}"
            )
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


def check_for_updates(config, selected_grant, **selection):
    status, msg, updates = process_aiida_configuration(
        config, config_path, selected_grant, **selection
    )
    return msg, updates if status else {}


def process_aiida_configuration(
    config,
    config_path,
    selected_grant,
    selected_computers=None,
    selected_codes=None,
    deactivate_computers=(),
    deactivate_codes=(),
    update_computers=False,
):
    """Inspect selected resources. Unlisted resources are kept unless explicitly chosen.

    Inspection has no profile, SSH, filesystem or remote mutation. All identifiers
    stored in the plan are plain values, never ORM objects.
    """
    try:
        computers, codes = select_resources(
            config, selected_grant, selected_computers, selected_codes
        )
        tasks = installation_tasks(config, computers, codes)
    except (KeyError, TypeError, ValueError) as error:
        return False, f"❌ {escape(str(error))}<br>", {}
    selected = {key: config["computers"][key] for key in computers}
    plan = {
        "selection": {"computers": sorted(computers), "codes": sorted(codes)},
        "tasks": tasks,
        "unmanaged": {"computers": [], "codes": {}},
        "blocked": [],
    }
    remote_selected = {
        key: data
        for key, data in selected.items()
        if data["setup"].get("transport", "core.ssh") != "core.local"
    }
    config_ok, msg, config_hosts = check_ssh_config(Path(config_path), remote_selected)
    if not config_ok:
        plan["ssh_config"] = {"rename": True, "hosts": config_hosts}
    status, computer_msg, active_computers, inactive_computers = aiida_computers()
    codes_ok, code_msg, active_codes, inactive_codes = aiida_codes()
    if not (status and codes_ok):
        return False, msg + computer_msg + code_msg, {}

    defined = config.get("computers", {})
    valid_computers = set(_configured_computer_labels(defined))
    valid_computers.update(data["setup"]["label"] for data in defined.values())
    valid_codes = {
        f"{data['label']}@{label}"
        for data in config.get("codes", {}).values()
        for label in _configured_computer_labels(
            {data["computer"]: defined[data["computer"]]}
        )
    }
    valid_codes.update(
        f"{data['label']}@{defined[data['computer']]['setup']['label']}"
        for data in config.get("codes", {}).values()
    )
    plan["unmanaged"]["computers"] = sorted(active_computers - valid_computers)
    plan["unmanaged"]["codes"] = {
        f"{label}@{computer}": pk
        for label, computer, pk in sorted(active_codes)
        if f"{label}@{computer}" not in valid_codes
    }
    if not set(deactivate_computers) <= set(plan["unmanaged"]["computers"]):
        return False, "❌ The computer choices changed. Inspect again.<br>", {}
    if not set(deactivate_codes) <= set(plan["unmanaged"]["codes"]):
        return False, "❌ The code choices changed. Inspect again.<br>", {}
    for label in deactivate_computers:
        _queue_update(plan, "computers", label, hide=True, rename=False, install=False)
    for label in deactivate_codes:
        _queue_update(
            plan,
            "codes",
            label,
            hide=True,
            rename=False,
            install=False,
            identifier=plan["unmanaged"]["codes"][label],
        )

    for key in sorted(computers):
        data = defined[key]
        label = data["setup"]["label"]
        if label in active_computers:
            equal, detail = compare_computer_configuration(label, data)
            msg += detail
            if equal:
                continue
            if not update_computers:
                msg += f"⚠️ Keeping the existing definition of {escape(label)}. Enable computer updates to replace it.<br>"
                continue
        _queue_update(
            plan,
            "computers",
            label,
            computer_key=key,
            hide=label in active_computers,
            rename=label in active_computers or label in inactive_computers,
            install=True,
        )

    installed_plugins = {ep.name for ep in entry_points(group="aiida.calculations")}
    for key in sorted(codes):
        data = config["codes"][key]
        plugin = data.get("default_calc_job_plugin")
        if plugin and plugin not in installed_plugins:
            plan["blocked"].append(
                f"Install the app/plugin providing {plugin} before configuring {key}."
            )
        computer = defined[data["computer"]]["setup"]["label"]
        label = f"{data['label']}@{computer}"
        active_pk = next(
            (pk for name, comp, pk in active_codes if f"{name}@{comp}" == label), None
        )
        inactive_pk = next(
            (pk for name, comp, pk in inactive_codes if f"{name}@{comp}" == label), None
        )
        replacing_computer = (
            plan.get("computers", {}).get(computer, {}).get("install", False)
        )
        if active_pk is not None and not replacing_computer:
            equal, detail = compare_code_configuration(
                label, {**data, "computer": computer}
            )
            msg += detail
            if equal:
                _queue_update(
                    plan, "codes", label, code_key=key, checkuenv=True, install=False
                )
                continue
        _queue_update(
            plan,
            "codes",
            label,
            code_key=key,
            hide=active_pk is not None,
            rename=active_pk or inactive_pk or False,
            install=True,
        )

    # Replacing a Computer also affects codes outside the chosen code subset.
    # Refuse that implicit side effect; never silently retire a kept code.
    for label, update in plan.get("computers", {}).items():
        if update.get("install") and update.get("rename"):
            kept = [
                f"{name}@{computer}"
                for name, computer, _ in active_codes
                if computer == label
                and f"{name}@{computer}" not in plan.get("codes", {})
            ]
            if kept:
                plan["blocked"].append(
                    f"Replacing {label} would affect kept codes: {', '.join(sorted(kept))}. "
                    "Select their managed definitions, explicitly deactivate unlisted "
                    "codes, or deselect this computer."
                )
    for section in ("computers", "codes"):
        for label, update in plan.get(section, {}).items():
            action = (
                "Install/replace"
                if update.get("install")
                else ("Check runtime" if update.get("checkuenv") else "Deactivate")
            )
            msg += f"⬜ {action}: {escape(label)}<br>"
    for task in tasks:
        msg += f"⬜ Install supporting files: {escape(task['name'])}<br>"
    for problem in plan["blocked"]:
        msg += f"❌ {escape(problem)}<br>"
    return True, msg, plan


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
            identifier=update.get("identifier"),
        )
        if not status:
            return False, uenvs

    return True, uenvs
