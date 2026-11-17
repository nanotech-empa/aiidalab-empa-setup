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
from .configuration import load_config_bundle, resolve_variables
from .applications import codes_for_applications, find_licensed_orca_archive, prepare_applications
from .external_codes import external_codes_html, inspect_external_codes
from .readiness import (
    manage_uenv_images as manage_uenv_images,  # public compatibility import
)
from .repo_utils import (
    BRANCH,
    GIT_REPO_PATH,
    clone_repository,
    config_path,
    configuration_file,
    get_latest_remote_commit,
    get_local_commit,
    pull_latest_changes,
    switch_config_branch,
)
from .selection import installation_tasks, select_resources

SUPPORTED_SCHEMA_VERSION = 5


# Check repository of config files
def check_repository(branch=BRANCH):
    """Synchronize the selected remote branch when the user requests inspection."""
    msg = "✅ Configuration synchronized. Review the inspected changes before applying."
    if not os.path.exists(GIT_REPO_PATH):
        msg = (
            "<b style='color:orange;'>⚠️ Repository updated. "
            "Review the inspected changes before applying.</b>"
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
            False,
            (
                "❌ Cannot synchronize the selected configuration branch "
                f"'{escape(branch)}'. Check the connection and remote branch, then inspect again."
            ),
        )

    if local_commit != remote_commit:
        if not pull_latest_changes(branch=branch):
            return (
                False,
                "<b style='color:red;'>❌ Failed to update the repository.</b>",
            )
        if get_local_commit() != remote_commit:
            return (
                False,
                "❌ The local configuration has unpublished changes. They were preserved; reconcile the branch and inspect again.",
            )

    return True, msg


def get_config(
    file_path=None,
    config_widgets=None,
    branch=BRANCH,
    deployment="hosted",
    computer_grants=None,
    settings=None,
    selected_computers=None,
    install_local_runtimes=True,
    automatic_codes=False,
):
    """Render the configuration already on disk without fetching or switching Git."""
    file_path = file_path or configuration_file
    config_widgets = config_widgets or {}

    for key, widget in config_widgets.items():
        if widget.value == "select" and not (
            key == "cscsuser" and deployment == "personal" and not computer_grants
        ):
            return False, f"<b style='color:red;'>❌please select {key}</b>", {}

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

    variables = data.setdefault("variables", {})
    variables.update(settings or {})
    if automatic_codes:
        try:
            prepare_applications(data)
            find_licensed_orca_archive(data)
        except ValueError as error:
            return False, f"❌ {escape(str(error))}", {}
    # Run scripts from the exact inspected bundle, never a stale global path.
    variables["config_files"] = str(Path(file_path).resolve().parent)
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

    try:
        variables = resolve_variables(variables)
    except ValueError as error:
        return False, f"❌ {escape(str(error))}", {}
    data["variables"] = variables

    # These values enter nested SSH proxy shell commands. Validate before rendering.
    for key, spec in data.get("runtime_settings", {}).items():
        if selected_computers is not None and spec.get("computers") and not (
            set(spec["computers"]) & set(selected_computers)
        ):
            continue
        value = variables.get(key, "")
        if spec.get("format") == "positive_integer" and (
            not isinstance(value, str) or not value.isdecimal() or int(value) < 1
        ):
            return False, f"❌ {escape(key)} must be a positive integer.", {}
        if spec.get("format") == "local_file" and value and (
            not isinstance(value, str) or not Path(value).is_absolute()
            or "\n" in value or "\x00" in value
        ):
            return False, f"❌ {escape(key)} must be blank or an absolute local file path.", {}
        if spec.get("format") == "local_path" and (
            not isinstance(value, str)
            or not re.fullmatch(r"/[A-Za-z0-9_./+ -]+", value)
            or value == "/"
            or os.path.normpath(value) != value
        ):
            return False, f"❌ {escape(key)} must be a normalized absolute local path without shell expressions.", {}
        if spec.get("format") == "ssh_username" and (
            not isinstance(value, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9._-]*", value)
            or value == "select"
        ):
            return False, f"❌ Enter a valid {escape(spec.get('description', key))}.", {}
        if spec.get("format") == "ssh_key_path" and (
            not isinstance(value, str)
            or (value and not re.fullmatch(r"(?:~/|/)[A-Za-z0-9_./-]+", value))
        ):
            return False, f"❌ {escape(key)} must be blank or a key path on ELA beginning with ~/ or /, without spaces or shell characters.", {}

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
    for key, task in data.get("setup_tasks", {}).items():
        grant = account_map.get(task["computer"], widget_replacements.get("grant"))
        if grant is not None:
            data["setup_tasks"][key] = recursive_replace(task, {"grant": grant})
    data = recursive_replace(data, all_replacements)
    data["_deployment"] = deployment
    data["_install_local_runtimes"] = install_local_runtimes
    data["_computer_grants"] = account_map
    data["_metadata"] = {
        "schema_version": schema_version,
        "config_branch": branch,
        "config_revision": get_local_commit() or "unknown",
    }

    return True, "", data


def inspected_revision_is_current(config):
    """Apply only the local Git revision that was explicitly inspected."""
    revision = config.get("_metadata", {}).get("config_revision")
    return bool(revision and revision != "unknown" and revision == get_local_commit())


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
    if config.get("_automatic_codes"):
        plan["applications"] = config["_installed_applications"]
        if any(config["codes"][key].get("default_calc_job_plugin") == "orca.orca" and config["codes"][key]["computer"] == "mac-localhost" for key in codes):
            variables = config.get("variables", {})
            if not variables.get("local_orca_archive") and not Path(variables["local_orca_root"]).exists():
                plan["blocked"].append("ORCA needs your licensed vendor archive. Upload it to Work using AiiDAlab Files, then inspect again.")
    remote_selected = {
        key: data
        for key, data in selected.items()
        if data["setup"].get("transport", "core.ssh") != "core.local"
    }
    config_ok, msg, config_hosts = check_ssh_config(Path(config_path), remote_selected, config.get("ssh_config", {}))
    if not config_ok:
        plan["ssh_config"] = {"rename": True, "hosts": config_hosts}
    status, computer_msg, active_computers, inactive_computers = aiida_computers()
    codes_ok, code_msg, active_codes, inactive_codes = aiida_codes()
    if not (status and codes_ok):
        return False, msg + computer_msg + code_msg, {}

    providers, external_pks = inspect_external_codes(
        config, active_codes, inactive_codes, active_computers, computers
    )
    plan["external_code_providers"] = providers
    msg += external_codes_html(providers, compact=bool(config.get('_automatic_codes')))

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
    if config.get("_automatic_codes"):
        # Codes belonging to apps that are no longer installed are unexpected,
        # even if a definition still exists in the full multi-app catalog.
        expected_keys = codes_for_applications(
            config, config["_installed_applications"], set(defined)
        )
        valid_codes = set()
        for key in expected_keys:
            data = config['codes'][key]
            labels = set(_configured_computer_labels({data['computer']: defined[data['computer']]}))
            labels.add(defined[data['computer']]['setup']['label'])
            valid_codes.update(f"{data['label']}@{label}" for label in labels)
    plan["unmanaged"]["computers"] = sorted(active_computers - valid_computers)
    plan["unmanaged"]["codes"] = {
        f"{label}@{computer}": pk
        for label, computer, pk in sorted(active_codes)
        if f"{label}@{computer}" not in valid_codes and pk not in external_pks
    }
    comparisons = {}
    computer_comparisons = {}
    if config.get("_automatic_codes"):
        plan["replacements"] = {}
        for key in computers:
            data = defined[key]
            label = data['setup']['label']
            if label in active_computers:
                computer_comparisons[label] = compare_computer_configuration(label, data)
        for key in sorted(codes):
            data = config['codes'][key]
            computer = defined[data['computer']]['setup']['label']
            label = f"{data['label']}@{computer}"
            active_pk = next((pk for name, comp, pk in active_codes if f"{name}@{comp}" == label), None)
            if active_pk is not None:
                comparisons[label] = compare_code_configuration(label, {**data, "computer": computer})
                if not comparisons[label][0] or (update_computers and computer in computer_comparisons and not computer_comparisons[computer][0]):
                    plan['unmanaged']['codes'][label] = active_pk
                    plan['replacements'][label] = active_pk
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
            equal, detail = computer_comparisons.get(label) or compare_computer_configuration(label, data)
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
            equal, detail = comparisons.get(label) or compare_code_configuration(label, {**data, "computer": computer})
            msg += detail
            if equal:
                _queue_update(
                    plan, "codes", label, code_key=key, checkuenv=True, install=False
                )
                continue
        if config.get('_automatic_codes') and active_pk is not None and label not in deactivate_codes:
            plan['blocked'].append(f"Keeping {label}. To replace this incompatible registration, select its Hide checkbox; it will be renamed only to free the required label. Existing calculation data is retained.")
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
            if config.get("_automatic_codes") and section == "codes":
                continue
            action = (
                "Install/replace"
                if update.get("install")
                else ("Check runtime" if update.get("checkuenv") else "Deactivate")
            )
            name = label
            if config.get('_automatic_codes') and update.get('computer_key'):
                name = defined[update['computer_key']].get('display_name', label)
            msg += f"⬜ {action}: {escape(name)}<br>"
    if config.get("_automatic_codes"):
        msg += f"⬜ Configure {len(codes)} required code registrations and {len(tasks)} supporting runtimes for the detected apps.<br>"
    else:
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
