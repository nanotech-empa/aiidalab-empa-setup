import asyncio
import functools
from datetime import datetime

import ipywidgets as ipw
import yaml

from empa_setup_utils.aiida_and_ssh_utils import (
    execute_custom_commands,
    get_old_unfinished_workchains,
    key_is_valid,
    play_paused_workchains,
    set_ssh,
    update_ssh_config,
)
from empa_setup_utils.control import (
    check_for_updates,
    check_repository,
    config_path,
    get_config,
    manage_uenv_images,
    setup_codes,
    setup_computers,
)
from empa_setup_utils.repo_utils import BRANCH, available_config_branches
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
        self.config = {}
        self.updates_needed = {}
        self.config_branch_widget = ipw.Dropdown(
            description="Config branch",
            options=available_config_branches(),
            value=BRANCH,
            style={"description_width": "110px"},
            layout=ipw.Layout(width="430px"),
        )
        self.config_branch_widget.observe(self.on_config_branch_change, names="value")

        self.check_button = ipw.Button(description="Inspect updates", button_style="info")
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
                self.selectors,
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
                self.update_message.value = f"<b>{timestamp}</b>: {remove_green_check_lines(msg)}"
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

    def on_config_branch_change(self, change):
        """Reload YAML-driven selector widgets after switching config branch."""
        if change["old"] == change["new"]:
            return

        self.start_button.disabled = True
        self.config_widgets = self.widgets_from_yaml(branch=change["new"])
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
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        yaml_widgets = data.get("widgets", {})
        return {
            key: ipw.Dropdown(description=key, options=options)
            for key, options in yaml_widgets.items()
        }

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

    def check_for_all_updates(self, _):
        status_ok, msg, self.config = get_config(
            config_widgets=self.config_widgets,
            branch=self.config_branch_widget.value,
        )
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not status_ok:
            self.update_message.value = f"<b>{timestamp}</b>: {msg}"
            return
        config_details = self.config_details_html()
        ssh_key_updated = key_is_valid(
            public_key_file=self.config["variables"]["ssh_public_key"]
        )
        if not ssh_key_updated:
            self.update_message.value = (
                f"<b>{timestamp}</b>: {config_details}"
                "❌ SSH key is not valid, please update it"
            )
            return
        if msg == "":
            msg, self.updates_needed = check_for_updates(
                self.config, self.config_widgets["grant"].value
            )
        msg = remove_green_check_lines(msg)
        if not msg:
            self.update_message.value = (
                f"<b>{timestamp}</b>: {config_details}✅ Nothing to report"
            )
        else:
            self.update_message.value = (
                f"<b>{timestamp}</b>: {config_details}{remove_green_check_lines(msg)}"
            )

        _, msg = get_old_unfinished_workchains()
        self.update_old_workchains.value = f"<b>Old WorkChains Check:</b> {msg}"
        somerunning, msg = get_old_unfinished_workchains(cutoffdays=3, reverse=True)
        if somerunning:
            self.running_workchains.value = (
                f"<b>There are running workchains, you cannot update:</b> {msg}"
            )
        else:
            self.start_button.disabled = not bool(self.updates_needed)

    def clear_output(self, _):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.output.clear_output()
        self.update_message.value = f"<b>{timestamp}</b>: ✅ Nothing to report"
        self.subtitle.value = ""
        self.paused_workchains.value = ""
        self.start_button.disabled = True

    def run_configuration(self, _):
        self.check = False
        self.output.clear_output()

        self.subtitle.value = "<h3>Setup SSH config file. Check SSH connection.</h3>"
        with self.output:
            if "ssh_config" in self.updates_needed:
                update_ssh_config(
                    config_path,
                    self.config["ssh_config"],
                    rename=self.updates_needed["ssh_config"]["rename"],
                )
                if not set_ssh(
                    self.config["computers"],
                    self.updates_needed["ssh_config"]["hosts"],
                ):
                    print("❌ ssh problem, ask for support")
                    return
            print("✅ ssh setup done")

        self.subtitle.value = "<h3>Setup computers</h3>"
        with self.output:
            print("🔄 Setting up computers")
            status = setup_computers(
                self.updates_needed.get("computers", {}), self.config["computers"]
            )
            if not status:
                return
            print("✅ Done")

        self.subtitle.value = "<h3>Setup Codes and Uenvs. It will take several minutes</h3>"
        with self.output:
            print("🔄 Setting up codes")
            status, uenvs = setup_codes(
                self.updates_needed.get("codes", {}), self.config
            )
            if not status:
                return
            if len(uenvs) > 0:
                uenvs_ok = manage_uenv_images(uenvs)
                if not uenvs_ok:
                    print("❌ uenvs not set up correctly ask for help")
                    return
            print("✅ Done")
        self.subtitle.value = "<h3>Additional commands</h3>"
        with self.output:
            print("🔄 Executing final commands")
            status_ok = execute_custom_commands(self.config)
            if not status_ok:
                print("❌ custom commands not set up correctly ask for help")
                return
            print("✅ Done")
        self.start_button.disabled = True
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.update_message.value = f"<b>{timestamp}</b>: ✅ Nothing to report"
        return


def get_start_widget(appbase, jupbase, notebase):
    return ConfigAiiDAlabApp()
