import asyncio
import functools
import os
import platform
from datetime import datetime

import ipywidgets as ipw

from empa_setup_utils.aiida_and_ssh_utils import (
    execute_custom_commands,
    execute_setup_tasks,
    get_old_unfinished_workchains,
    key_is_valid,
    play_paused_workchains,
    set_ssh,
    unfinished_processes_for_computers,
    update_ssh_config,
)
from empa_setup_utils.configuration import load_config_bundle
from empa_setup_utils.control import (
    check_for_updates,
    check_repository,
    config_path,
    get_config,
    manage_uenv_images,
    setup_codes,
    setup_computers,
)
from empa_setup_utils.readiness import code_runtimes
from empa_setup_utils.repo_utils import BRANCH, available_config_branches
from empa_setup_utils.selection import selected_ssh_config
from empa_setup_utils.string_utils import remove_green_check_lines

__version__ = "v2025.0214"


class ConfigAiiDAlabApp(ipw.VBox):
    def __init__(self):
        self.title = ipw.HTML("<h2>Config AiiDAlab Application</h2>")

        self.update_message = ipw.HTML("Nothing to report")
        self.update_old_workchains = ipw.HTML("")
        self.running_workchains = ipw.HTML("")
        self.paused_workchains = ipw.HTML("")
        self.check = True  # Disabled while applying updates.
        self.catalog = {}
        self.config = {}
        self.updates_needed = {}
        self.deployment = ipw.Dropdown(
            description="Deployment",
            options=[("CSCS Kubernetes", "hosted"), ("Personal Docker", "personal")],
            value="hosted" if os.environ.get("KUBERNETES_SERVICE_HOST") else "personal",
            style={"description_width": "110px"},
            layout=ipw.Layout(width="430px"),
        )
        self.deployment.observe(self.on_context_change, names="value")
        self.update_computer_definitions = ipw.Checkbox(
            value=False,
            description="Update existing computer definitions",
            indent=False,
            layout=ipw.Layout(width="95%"),
        )
        self.update_computer_definitions.observe(self.invalidate_plan, names="value")
        self.account_widgets = {}
        self.account_selectors = ipw.VBox()
        self.runtime_settings = {}
        self.runtime_setting_box = ipw.VBox()
        self.computer_selection = ipw.SelectMultiple(
            description="Computers", options=[], rows=4
        )
        self.code_selection = ipw.SelectMultiple(
            description="Codes", options=[], rows=8, layout=ipw.Layout(width="95%")
        )
        self.computer_selection.observe(self.on_computers_change, names="value")
        self.code_selection.observe(self.invalidate_plan, names="value")
        self.unmanaged_choices = ipw.VBox()
        self.deactivate_computers = {}
        self.deactivate_codes = {}
        self.all_computers_button = ipw.Button(description="All computers")
        self.all_computers_button.on_click(
            lambda _: setattr(
                self.computer_selection, "value", tuple(self.computer_selection.options)
            )
        )
        self.all_codes_button = ipw.Button(description="All codes")
        self.all_codes_button.on_click(
            lambda _: setattr(
                self.code_selection, "value", tuple(self.code_selection.options)
            )
        )
        self.config_branch_widget = ipw.Dropdown(
            description="Config branch",
            options=available_config_branches(),
            value=BRANCH,
            style={"description_width": "110px"},
            layout=ipw.Layout(width="430px"),
        )
        self.config_branch_widget.observe(self.on_config_branch_change, names="value")

        self.check_button = ipw.Button(
            description="Inspect updates", button_style="info"
        )
        self.check_button.on_click(self.check_for_all_updates)

        self.start_button = ipw.Button(
            description="Apply updates", button_style="primary", disabled=True
        )
        self.start_button.on_click(self.run_configuration)

        self.clear_button = ipw.Button(description="Clear logs", button_style="warning")
        self.clear_button.on_click(self.clear_output)

        self.play_button = ipw.Button(
            description="Play paused workchains", button_style="success", disabled=True
        )
        self.play_button.on_click(self.play_paused)

        self.subtitle = ipw.HTML("")
        self.output = ipw.Output()

        self.config_widgets = self.widgets_from_yaml(
            branch=self.config_branch_widget.value
        )
        self.refresh_resource_choices()
        some_paused_calculations, self.paused_calculations = (
            self.check_paused_workchains()
        )
        self.play_button.disabled = not some_paused_calculations

        controls = ipw.HBox(
            [self.check_button, self.start_button, self.play_button, self.clear_button]
        )
        self.selectors = (
            ipw.HBox(list(self.config_widgets.values()))
            if self.config_widgets
            else ipw.HBox([])
        )

        super().__init__(
            [
                self.title,
                self.update_old_workchains,
                self.running_workchains,
                self.paused_workchains,
                self.update_message,
                self.config_branch_widget,
                self.deployment,
                ipw.HTML(
                    f"Container architecture: <b>{platform.machine()}</b>. Local codes run inside this Linux container."
                ),
                self.selectors,
                ipw.HTML(
                    "<b>Choose what to configure</b><br>Select one or more entries (Cmd/Ctrl-click). Unselected resources are kept."
                ),
                self.computer_selection,
                self.all_computers_button,
                self.account_selectors,
                self.update_computer_definitions,
                self.code_selection,
                self.all_codes_button,
                self.runtime_setting_box,
                self.unmanaged_choices,
                controls,
                self.subtitle,
                self.output,
            ]
        )

        # Start periodic checks
        # asyncio.create_task(self._start_periodic_check_updates(60))
        # asyncio.create_task(self._start_periodic_check_old_workchains(60))

    async def _start_periodic_check_updates(self, interval):
        """Periodically check for updates."""
        while True:
            if self.check:
                status_ok, msg, self.config = await asyncio.to_thread(
                    functools.partial(
                        get_config,
                        config_widgets=self.config_widgets,
                        branch=self.config_branch_widget.value,
                    )
                )
                if status_ok:
                    msg, self.updates_needed = await asyncio.to_thread(
                        functools.partial(
                            check_for_updates,
                            self.config,
                            self.config_widgets["grant"].value,
                        )
                    )
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = remove_green_check_lines(msg)
                if not msg:
                    msg = "✅ Nothing to report"
                self.update_message.value = (
                    f"<b>{timestamp}</b>: {remove_green_check_lines(msg)}"
                )
            await asyncio.sleep(interval)

    async def _start_periodic_check_old_workchains(self, interval):
        """Periodically check for pending too old workchains."""
        while True:
            if self.check:
                update_result = await asyncio.to_thread(get_old_unfinished_workchains)
                self.update_old_workchains.value = (
                    f"<b>Old WorkChains Check:</b> {update_result}"
                )
            await asyncio.sleep(interval)

    def invalidate_plan(self, _=None):
        """Require a fresh inspection after any setup selector changes."""
        self.start_button.disabled = True
        self.updates_needed = {}

    def on_config_branch_change(self, change):
        """Reload YAML-driven selector widgets after switching config branch."""
        if change["old"] == change["new"]:
            return

        self.invalidate_plan()
        self.config = {}
        self.config_widgets = self.widgets_from_yaml(branch=change["new"])
        self.refresh_resource_choices()
        self.deactivate_computers = {}
        self.deactivate_codes = {}
        self.unmanaged_choices.children = ()
        self.selectors.children = (
            tuple(self.config_widgets.values()) if self.config_widgets else ()
        )

    def widgets_from_yaml(
        self,
        file_path="/home/jovyan/opt/aiidalab-alps-files/config.yml",
        branch=BRANCH,
    ):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_ok, msg = check_repository(branch=branch)
        if not status_ok:
            self.update_message.value = f"<b>{timestamp}</b>: {msg}"
            return None
        self.update_message.value = f"<b>{timestamp}</b>: {msg}"
        data = load_config_bundle(file_path)

        self.catalog = data
        yaml_widgets = data.get("widgets", {})
        widgets = {
            key: ipw.Dropdown(description=key, options=options)
            for key, options in yaml_widgets.items()
        }
        for widget in widgets.values():
            widget.observe(self.on_context_change, names="value")
        return widgets

    def check_paused_workchains(self):
        """Check for paused workchains."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        some_paused, msg = get_old_unfinished_workchains(
            cutoffdays=4, reverse=True, paused=True
        )
        if msg != "":
            self.paused_workchains.value = (
                f"<b>{timestamp}</b>: There are paused workchains: {msg}"
            )
        return some_paused, msg

    def config_details_html(self):
        metadata = self.config.get("_metadata", {})
        if not metadata:
            return ""

        schema_version = metadata.get("schema_version", "unknown")
        config_branch = metadata.get("config_branch", "unknown")
        config_revision = metadata.get("config_revision", "unknown")
        return (
            "<small>"
            f"Config schema: {schema_version}; "
            f"branch: {config_branch}; "
            f"config revision: {config_revision}"
            "</small><br>"
        )

    def play_paused(self, _):
        self.play_button.disabled = True
        success = True
        output = "No paused workchains to play."
        if self.paused_calculations != "":
            output, success = play_paused_workchains(self.paused_calculations)
        if success:
            self.paused_workchains.value = f"<b>{output}</b>: Workchains are resumed"
        else:
            self.paused_workchains.value = (
                f"<b>{output}</b>: Workchains are not resumed, please check"
            )

    def on_context_change(self, _):
        self.invalidate_plan()
        self.refresh_resource_choices()
        self.deactivate_computers = {}
        self.deactivate_codes = {}
        self.unmanaged_choices.children = ()

    def refresh_resource_choices(self):
        grant_widget = self.config_widgets.get("grant") if self.config_widgets else None
        eligible = tuple(
            key
            for key, data in self.catalog.get("computers", {}).items()
            if self.deployment.value in data.get("deployments", ["hosted", "personal"])
            and (not grant_widget or grant_widget.value in data.get("grants", []))
        )
        self.account_widgets = {}
        if not grant_widget:
            for key in eligible:
                grants = self.catalog["computers"][key].get("grants", [])
                if grants:
                    widget = ipw.Dropdown(
                        description=f"{key} account",
                        options=grants if len(grants) == 1 else ["select", *grants],
                        style={"description_width": "160px"},
                        layout=ipw.Layout(width="390px"),
                    )
                    widget.observe(self.invalidate_plan, names="value")
                    self.account_widgets[key] = widget
        self.computer_selection.options = eligible
        self.computer_selection.value = eligible
        self.on_computers_change(None)
        self.runtime_settings = {}
        settings = []
        for key, spec in self.catalog.get("runtime_settings", {}).items():
            if self.deployment.value not in spec.get(
                "deployments", ["hosted", "personal"]
            ):
                continue
            widget = ipw.Text(
                description=spec.get("description", key),
                value=str(self.catalog.get("variables", {}).get(key, "")),
                style={"description_width": "210px"},
                layout=ipw.Layout(width="95%"),
            )
            widget.observe(self.invalidate_plan, names="value")
            self.runtime_settings[key] = widget
            settings.append(widget)
        self.runtime_setting_box.children = tuple(settings)

    def on_computers_change(self, _):
        self.invalidate_plan()
        options = tuple(
            key
            for key, data in self.catalog.get("codes", {}).items()
            if data["computer"] in self.computer_selection.value
            and self.deployment.value in data.get("deployments", ["hosted", "personal"])
            and platform.machine() in data.get("architectures", [platform.machine()])
        )
        self.code_selection.options = options
        self.code_selection.value = options
        self.account_selectors.children = tuple(
            widget
            for key, widget in self.account_widgets.items()
            if key in self.computer_selection.value
        )

    def config_arguments(self):
        return {
            "config_widgets": self.config_widgets,
            "branch": self.config_branch_widget.value,
            "deployment": self.deployment.value,
            "computer_grants": {
                key: widget.value
                for key, widget in self.account_widgets.items()
                if key in self.computer_selection.value
            },
            "settings": {
                key: widget.value for key, widget in self.runtime_settings.items()
            },
        }

    def selected_grant(self):
        widget = self.config_widgets.get("grant") if self.config_widgets else None
        return widget.value if widget else None

    def selection_arguments(self):
        return {
            "update_computers": self.update_computer_definitions.value,
            "selected_computers": self.computer_selection.value,
            "selected_codes": self.code_selection.value,
            "deactivate_computers": tuple(
                key for key, widget in self.deactivate_computers.items() if widget.value
            ),
            "deactivate_codes": tuple(
                key for key, widget in self.deactivate_codes.items() if widget.value
            ),
        }

    def affected_computer_labels(self):
        labels = {
            self.config["computers"][key]["setup"]["label"]
            for key in self.computer_selection.value
        }
        labels.update(
            key for key, widget in self.deactivate_computers.items() if widget.value
        )
        labels.update(
            key.rsplit("@", 1)[-1]
            for key, widget in self.deactivate_codes.items()
            if widget.value
        )
        return labels

    def show_unmanaged_choices(self):
        unmanaged = self.updates_needed.get("unmanaged", {})
        rows = []
        for section, choices in (
            ("computers", self.deactivate_computers),
            ("codes", self.deactivate_codes),
        ):
            current = {}
            for label in unmanaged.get(section, []):
                widget = choices.get(label) or ipw.Checkbox(
                    value=False,
                    description=f"Deactivate {label}",
                    indent=False,
                    layout=ipw.Layout(width="100%"),
                )
                if label not in choices:
                    widget.observe(self.invalidate_plan, names="value")
                current[label] = widget
                rows.append(widget)
            choices.clear()
            choices.update(current)
        if rows:
            rows.insert(
                0,
                ipw.HTML(
                    "<b>These codes/computers are not supported by this configuration. Keep them or deactivate?</b><br>"
                    "Keep is the default. Check only those to deactivate, then inspect again. "
                    "Disabling a computer prevents new use of all codes on it. "
                    "Existing calculation data is retained."
                ),
            )
        self.unmanaged_choices.children = tuple(rows)

    def check_for_all_updates(self, _):
        self.invalidate_plan()
        if any(
            widget.value == "select"
            for key, widget in self.account_widgets.items()
            if key in self.computer_selection.value
        ):
            self.update_message.value = (
                "Select an account for each chosen remote computer."
            )
            return
        status_ok, msg, self.config = get_config(**self.config_arguments())
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not status_ok:
            self.update_message.value = f"<b>{timestamp}</b>: {msg}"
            return
        # A local-only deactivation plan needs no CSCS certificate.
        remote_selected = any(
            self.config["computers"][key]["setup"]["transport"] != "core.local"
            for key in self.computer_selection.value
        )
        if remote_selected and not key_is_valid(
            public_key_file=self.config.get("variables", {}).get("ssh_public_key", "")
        ):
            self.update_message.value = (
                "❌ Renew your CSCS certificate in the MFA app, then inspect again."
            )
            return
        msg, self.updates_needed = check_for_updates(
            self.config, self.selected_grant(), **self.selection_arguments()
        )
        self.show_unmanaged_choices()
        self.update_message.value = (
            f"<b>{timestamp}</b>: {self.config_details_html()}{msg}"
        )
        # Check only affected resources, including long-lived or paused users.
        running, detail = unfinished_processes_for_computers(
            self.affected_computer_labels()
        )
        self.running_workchains.value = (
            f"<b>Selected resources have unfinished processes; finish or review them before applying:</b> {detail}"
            if running
            else ""
        )
        actions = any(
            self.updates_needed.get(key)
            for key in ("computers", "codes", "ssh_config", "tasks")
        )
        self.start_button.disabled = (
            running or not actions or bool(self.updates_needed.get("blocked"))
        )

    def clear_output(self, _):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.output.clear_output()
        self.update_message.value = f"<b>{timestamp}</b>: ✅ Nothing to report"
        self.subtitle.value = ""
        self.paused_workchains.value = ""
        self.start_button.disabled = True

    def run_configuration(self, _):
        self.check = False
        self.start_button.disabled = True
        try:
            self._apply_configuration()
        finally:
            self.check = True
            self.updates_needed = {}

    def _apply_configuration(self):
        self.output.clear_output()
        # Reinspect immediately to reject stale resource choices and new running work.
        msg, fresh = check_for_updates(
            self.config, self.selected_grant(), **self.selection_arguments()
        )
        running, _ = unfinished_processes_for_computers(self.affected_computer_labels())
        if (
            not fresh
            or "selection" not in fresh
            or running
            or fresh != self.updates_needed
            or fresh.get("blocked")
        ):
            self.update_message.value = "Configuration changed or work is unfinished. Inspect again before applying."
            return
        chosen = fresh.get("selection", {})
        computers = chosen.get("computers", [])
        self.subtitle.value = "<h3>Check SSH and selected runtimes</h3>"
        with self.output:
            if computers:
                remote_computers = [
                    key
                    for key in computers
                    if self.config["computers"][key]["setup"]["transport"]
                    != "core.local"
                ]
                ssh_settings = selected_ssh_config(self.config, remote_computers)
                if ssh_settings:
                    update_ssh_config(config_path, ssh_settings)
                if not set_ssh(self.config["computers"], computers):
                    self.update_message.value = "❌ SSH check failed. Follow the diagnostic above and inspect again."
                    return
            requirements = code_runtimes(self.config, chosen.get("codes", []))
            images = list(
                dict.fromkeys(
                    (item["host"], item["image"], item["view"], None)
                    for item in requirements
                    if item["image"]
                )
            )
            if images and not manage_uenv_images(images):
                self.update_message.value = "❌ uenv mount check failed before installation. See diagnostics and inspect again."
                return
            if not execute_setup_tasks(fresh.get("tasks", []), self.config):
                self.update_message.value = "❌ Supporting-file installation failed. Inspect again after resolving it."
                return
            if not manage_uenv_images(requirements):
                self.update_message.value = "❌ Runtime check failed. AiiDA registrations have not been changed. See diagnostics and inspect again."
                return
            if "setup_tasks" not in self.config and not execute_custom_commands(
                self.config
            ):
                self.update_message.value = (
                    "❌ Additional installation commands failed."
                )
                return
            self.subtitle.value = "<h3>Register selected computers and codes</h3>"
            if not setup_computers(
                fresh.get("computers", {}), self.config["computers"]
            ):
                self.update_message.value = "❌ Computer setup failed. Inspect the current state before retrying."
                return
            ok, _ = setup_codes(fresh.get("codes", {}), self.config)
            if not ok:
                self.update_message.value = (
                    "❌ Code setup failed. Inspect the current state before retrying."
                )
                return
        self.update_message.value = (
            "✅ Selected configuration applied and login-host runtime checks passed. "
            "Run computer/code tests and a small scheduler job before production use."
        )
        self.subtitle.value = ""


def get_start_widget(appbase, jupbase, notebase):
    return ConfigAiiDAlabApp()
