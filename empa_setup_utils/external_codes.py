"""Recognize app-provided Code registrations without installing or changing them."""

from html import escape

from aiida.orm import InstalledCode, load_node


def inspect_external_codes(
    config, active_codes, inactive_codes, active_computers, selected
):
    """Return plain provider status and protected PKs; no executable/profile changes.

    Labels alone do not establish compatibility: require an InstalledCode on the
    configured local Computer and the declared calculation plugin. Presence is
    registration evidence, not an executable/version or scientific runtime test.
    """
    reports, protected = {}, set()
    managed = {
        (data["label"], config["computers"][data["computer"]]["setup"]["label"])
        for data in config.get("codes", {}).values()
    }
    for key, spec in config.get("external_code_providers", {}).items():
        computer_key = spec["computer"]
        computer = config["computers"][computer_key]["setup"]
        if computer.get("transport") != "core.local":
            continue
        plugins = spec["codes"]
        present, unavailable, found = {}, {}, set()
        for label, computer_label, pk in sorted(active_codes | inactive_codes):
            if computer_label != computer["label"]:
                continue
            if (label, computer_label) in managed:
                continue
            code = load_node(pk)
            if (
                not isinstance(code, InstalledCode)
                or code.computer.transport_type != "core.local"
            ):
                continue
            plugin = code.default_calc_job_plugin
            roles = {name for name, expected in plugins.items() if expected == plugin}
            if not roles:
                continue
            full_label = f"{label}@{computer_label}"
            protected.add(pk)
            if (
                label,
                computer_label,
                pk,
            ) in active_codes and computer_label in active_computers:
                present[full_label] = pk
                found.update(roles)
            else:
                unavailable[full_label] = pk
        if (
            present
            or unavailable
            or (computer_key in selected and spec.get("show_when_missing", True))
        ):
            reports[key] = {
                "title": spec["title"],
                "guidance": spec["guidance"],
                "url": spec["url"],
                "present": present,
                "unavailable": unavailable,
                "missing": sorted(set(plugins) - found),
            }
    return reports, protected


def external_codes_html(reports):
    """Explain provider ownership and optional next steps in the inspected plan."""
    sections = []
    for report in reports.values():
        text = f"<b>{escape(report['title'])}</b><br>"
        if report["present"]:
            text += (
                "Existing Code registrations, kept unchanged: "
                + ", ".join(escape(label) for label in report["present"])
                + ".<br>"
            )
        if report["unavailable"]:
            text += (
                "Hidden codes or codes on a disabled computer, kept unchanged: "
                + ", ".join(escape(label) for label in report["unavailable"])
                + ". These are not counted as available.<br>"
            )
        if report["missing"]:
            text += (
                "Not found as active local registrations: "
                + ", ".join(escape(name) for name in report["missing"])
                + f".<br>{escape(report['guidance'])} "
            )
            text += f'<a href="{escape(report["url"], quote=True)}" target="_blank" rel="noopener noreferrer">App instructions</a>.<br>'
        text += "EMPA setup does not modify these registrations. Executables and versions have not been probed by this inspection.<br>"
        sections.append(text)
    return "<br>".join(sections)
