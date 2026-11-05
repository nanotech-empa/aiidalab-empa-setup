"""Plain-data selection and installation-task planning (no profile or SSH access)."""

import platform
from copy import deepcopy


def select_resources(config, grant, computers=None, codes=None):
    """Validate explicit keys; None selects all eligible resources, [] selects none."""
    eligible = {
        key
        for key, data in config.get("computers", {}).items()
        if (
            not data.get("grants")
            or config.get("_computer_grants", {}).get(key, grant)
            in data.get("grants", [])
        )
        and config.get("_deployment", "hosted")
        in data.get("deployments", ["hosted", "personal"])
    }
    computers = eligible if computers is None else set(computers)
    if not computers <= eligible:
        raise ValueError("Select computers available for the chosen grant.")
    available_codes = {
        key
        for key, data in config.get("codes", {}).items()
        if data["computer"] in computers
    }
    codes = available_codes if codes is None else set(codes)
    for key in codes:
        data = config.get("codes", {}).get(key, {})
        if platform.machine() not in data.get("architectures", [platform.machine()]):
            raise ValueError(
                f"Code {key} is not available for container architecture {platform.machine()}."
            )
    if not codes <= available_codes:
        raise ValueError("Each selected code must belong to a selected computer.")
    return computers, codes


def selected_ssh_config(config, computers):
    """Include only selected targets and their explicitly declared SSH dependencies."""
    hosts = set()
    for key in computers:
        data = config["computers"][key]
        hosts.add(data.get("ssh_host", data["setup"]["hostname"]))
        hosts.update(data.get("ssh_dependencies", []))
        proxy = data.get("config", {}).get("proxy_jump")
        if proxy:
            hosts.add(proxy)
    return {
        host: data
        for host, data in config.get("ssh_config", {}).items()
        if host in hosts
    }


def installation_tasks(config, computers, codes):
    """Resolve schema-2 tasks; reject ambiguous legacy commands for partial installs."""
    if "setup_tasks" not in config:
        commands = config.get("custom_commands", {})
        if commands and (
            computers != set(config.get("computers", {}))
            or codes != set(config.get("codes", {}))
        ):
            raise ValueError(
                "This configuration has unscoped installation commands. Select a "
                "schema-2 configuration with setup_tasks before applying a partial setup."
            )
        return []
    tasks = []
    for name, task in config["setup_tasks"].items():
        computer = task["computer"]
        if computer not in config.get("computers", {}):
            raise ValueError(f"Installation task {name} refers to an unknown computer.")
        task_codes = set(task.get("codes", []))
        if any(
            key not in config.get("codes", {})
            or config["codes"][key]["computer"] != computer
            for key in task_codes
        ):
            raise ValueError(f"Installation task {name} refers to invalid codes.")
        if computer in computers and (not task_codes or task_codes & codes):
            tasks.append({"name": name, **deepcopy(task)})
    return tasks
