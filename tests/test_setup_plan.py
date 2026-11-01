from copy import deepcopy

import pytest

import start
from empa_setup_utils import aiida_and_ssh_utils, control


@pytest.fixture
def app(tmp_path, monkeypatch):
    path = tmp_path / "config.yml"
    path.write_text("widgets:\n  grant: [select, first, second]\n")
    monkeypatch.setattr(start, "available_config_branches", lambda: ["main"])
    monkeypatch.setattr(start, "check_repository", lambda **kwargs: (True, ""))
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp, "check_paused_workchains", lambda self: (False, "")
    )
    original = start.ConfigAiiDAlabApp.widgets_from_yaml
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp,
        "widgets_from_yaml",
        lambda self, **kwargs: original(self, file_path=path, **kwargs),
    )
    widget = start.ConfigAiiDAlabApp()
    yield widget
    widget.close()
    for selector in widget.config_widgets.values():
        selector.close()


def test_selector_change_invalidates_the_previously_inspected_plan(app):
    app.updates_needed = {"computers": {"first": {"install": True}}}
    app.start_button.disabled = False
    app.config_widgets["grant"].value = "second"
    assert app.start_button.disabled
    assert app.updates_needed == {}


def test_failed_reinspection_cannot_leave_apply_enabled(app, monkeypatch):
    app.updates_needed = {"codes": {"old": {"install": True}}}
    app.start_button.disabled = False
    monkeypatch.setattr(
        start, "get_config", lambda **kwargs: (False, "select a grant", {})
    )
    app.check_for_all_updates(None)
    assert app.start_button.disabled and not app.updates_needed


def test_failed_ssh_apply_requires_reinspection_and_restores_checks(app, monkeypatch):
    app.config = {"ssh_config": {}, "computers": {}}
    app.updates_needed = {"ssh_config": {"rename": False, "hosts": []}}
    app.start_button.disabled = False
    monkeypatch.setattr(start, "update_ssh_config", lambda *args, **kwargs: None)
    monkeypatch.setattr(start, "set_ssh", lambda *args: False)
    app.run_configuration(None)
    assert app.check
    assert app.start_button.disabled and not app.updates_needed


def test_inactive_code_plan_uses_its_identifier_without_modifying_yaml(
    monkeypatch, tmp_path
):
    config = {
        "computers": {
            "daint": {"setup": {"label": "daint_first"}, "grants": ["first"]}
        },
        "codes": {"pw": {"computer": "daint", "label": "pw"}},
    }
    original = deepcopy(config)
    monkeypatch.setattr(control, "check_ssh_config", lambda *args: (True, "", []))
    monkeypatch.setattr(
        control, "aiida_computers", lambda: (True, "", {"daint_first"}, set())
    )
    monkeypatch.setattr(
        control, "aiida_codes", lambda: (True, "", set(), {("pw", "daint_first", 42)})
    )
    monkeypatch.setattr(
        control, "compare_computer_configuration", lambda *args: (True, "")
    )
    success, _, plan = control.process_aiida_configuration(config, tmp_path, "first")
    assert success
    assert plan["codes"]["pw@daint_first"]["rename"] == 42
    assert config == original


def test_quoted_custom_commands_preserve_the_loaded_configuration(monkeypatch):
    config = {
        "custom_commands": {
            "remote_commands": {
                "remotehost": "test-host",
                "setup": [{"type": "ssh", "command": "echo 'two words'"}],
            },
            "local_commands": {"setup": [{"command": "echo 'two words'"}]},
        }
    }
    original = deepcopy(config)
    commands = []
    monkeypatch.setattr(
        aiida_and_ssh_utils,
        "run_command",
        lambda args: (commands.append(args) or "", True),
    )
    assert aiida_and_ssh_utils.execute_custom_commands(config)
    assert commands == [["echo", "two words"], ["ssh", "test-host", "echo 'two words'"]]
    assert config == original
