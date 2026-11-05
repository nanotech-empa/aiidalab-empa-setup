"""Only Inspect may synchronize configuration; Apply uses the reviewed revision."""

from copy import deepcopy

import pytest
import yaml

import start
from empa_setup_utils import control


@pytest.fixture
def cached_app(tmp_path, monkeypatch):
    path = tmp_path / "config.yml"
    data = {
        "schema_version": 2,
        "variables": {"prefix": "/default"},
        "runtime_settings": {"prefix": {"description": "Runtime prefix"}},
        "computers": {
            "alpha": {"setup": {"label": "alpha", "transport": "core.local"}},
            "beta": {"setup": {"label": "beta", "transport": "core.local"}},
        },
        "codes": {
            "first": {"computer": "alpha", "label": "first-v1"},
            "second": {"computer": "alpha", "label": "second"},
            "other": {"computer": "beta", "label": "other"},
        },
        "setup_tasks": {},
    }
    path.write_text(yaml.safe_dump(data))
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(
        start, "available_config_branches", lambda **kwargs: ["main", "preview"]
    )
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp, "check_paused_workchains", lambda self: (False, "")
    )
    monkeypatch.setattr(
        start, "unfinished_processes_for_computers", lambda labels: (False, "")
    )
    monkeypatch.setattr(control, "get_local_commit", lambda: "reviewed-revision")
    original = start.ConfigAiiDAlabApp.widgets_from_yaml
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp,
        "widgets_from_yaml",
        lambda self, **kwargs: original(self, **{**kwargs, "file_path": path}),
    )

    def forbidden(**kwargs):
        raise AssertionError("Startup, selector changes and Apply must not synchronize")

    monkeypatch.setattr(start, "check_repository", forbidden)
    monkeypatch.setattr(control, "check_repository", forbidden)
    widget = start.ConfigAiiDAlabApp()
    yield widget, path, data
    widget.close()


def test_opening_and_switching_branches_do_not_synchronize(cached_app):
    widget, _, _ = cached_app
    assert widget.catalog["codes"]["first"]["label"] == "first-v1"
    widget.config_branch_widget.value = "preview"
    assert "Inspect updates" in widget.update_message.value
    assert not widget.updates_needed and widget.start_button.disabled
    assert not hasattr(widget, "_start_periodic_check_updates")


def test_inspect_synchronizes_first_and_preserves_selection(cached_app, monkeypatch):
    widget, path, data = cached_app
    widget.computer_selection.value = ("alpha",)
    widget.code_selection.value = ("first",)
    widget.runtime_settings["prefix"].value = "/custom"
    events = []
    updated = deepcopy(data)
    updated["codes"]["first"]["label"] = "first-v2"
    updated["codes"]["new"] = {"computer": "alpha", "label": "new"}
    updated["computers"]["new-host"] = {
        "setup": {"label": "new-host", "transport": "core.local"}
    }

    def sync(**kwargs):
        events.append("sync")
        path.write_text(yaml.safe_dump(updated))
        return True, ""

    monkeypatch.setattr(start, "check_repository", sync)

    def inspect(config, grant, **selection):
        events.append("inspect")
        assert config["codes"]["first"]["label"] == "first-v2"
        assert config["variables"]["prefix"] == "/custom"
        assert selection["selected_computers"] == ("alpha",)
        assert selection["selected_codes"] == ("first",)
        return "Reviewed", {
            "selection": {"computers": ["alpha"], "codes": ["first"]},
            "codes": {"first-v2@alpha": {"install": True}},
        }

    monkeypatch.setattr(start, "check_for_updates", inspect)
    widget.check_for_all_updates(None)
    assert events == ["sync", "inspect"]
    assert not widget.start_button.disabled
    assert (
        "new" in widget.code_selection.options
        and "new" not in widget.code_selection.value
    )
    assert "new-host" not in widget.computer_selection.value


def test_failed_sync_stops_before_reading_and_inspecting(cached_app, monkeypatch):
    widget, _, _ = cached_app
    widget.updates_needed = {"codes": {"old": {"install": True}}}
    widget.start_button.disabled = False
    monkeypatch.setattr(
        start, "check_repository", lambda **kwargs: (False, "Cannot synchronize")
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("Failed synchronization must stop inspection")

    monkeypatch.setattr(start, "get_config", forbidden)
    monkeypatch.setattr(start, "check_for_updates", forbidden)
    widget.check_for_all_updates(None)
    assert widget.update_message.value == "Cannot synchronize"
    assert widget.start_button.disabled and not widget.updates_needed


def test_missing_cached_config_is_downloaded_only_on_inspect(cached_app, monkeypatch):
    widget, path, data = cached_app
    path.unlink()
    fresh = start.ConfigAiiDAlabApp()
    try:
        assert not fresh.catalog and not fresh.computer_selection.options
        assert "Inspect updates" in fresh.update_message.value

        def sync(**kwargs):
            path.write_text(yaml.safe_dump(data))
            return True, ""

        monkeypatch.setattr(start, "check_repository", sync)
        assert fresh.synchronize_for_inspection()
        assert path.is_file() and "alpha" in fresh.computer_selection.value
    finally:
        fresh.close()


def test_sync_preserves_compatible_accounts_and_runtime_overrides(
    cached_app, monkeypatch
):
    widget, path, data = cached_app
    data["computers"]["alpha"]["grants"] = ["first", "second"]
    path.write_text(yaml.safe_dump(data))
    widget.config_widgets = widget.widgets_from_yaml(file_path=path)
    widget.refresh_resource_choices()
    widget.computer_selection.value = ("alpha",)
    widget.account_widgets["alpha"].value = "second"
    widget.code_selection.value = ("first",)
    widget.runtime_settings["prefix"].value = "/custom"
    updated = deepcopy(data)
    updated["computers"]["alpha"]["grants"].append("third")
    updated["variables"]["prefix"] = "/new-default"

    def sync(**kwargs):
        path.write_text(yaml.safe_dump(updated))
        return True, ""

    monkeypatch.setattr(start, "check_repository", sync)
    assert widget.synchronize_for_inspection()
    assert widget.account_widgets["alpha"].value == "second"
    assert widget.runtime_settings["prefix"].value == "/custom"
    assert widget.code_selection.value == ("first",)


def test_removed_selected_code_requires_review(cached_app, monkeypatch):
    widget, path, data = cached_app
    widget.computer_selection.value = ("alpha",)
    widget.code_selection.value = ("first",)
    updated = deepcopy(data)
    del updated["codes"]["first"]

    def sync(**kwargs):
        path.write_text(yaml.safe_dump(updated))
        return True, ""

    monkeypatch.setattr(start, "check_repository", sync)

    def forbidden(*args, **kwargs):
        raise AssertionError("Removed choices need review before a new plan")

    monkeypatch.setattr(start, "check_for_updates", forbidden)
    widget.check_for_all_updates(None)
    assert "review your selections" in widget.update_message.value
    assert widget.start_button.disabled and not widget.updates_needed


def test_apply_checks_local_revision_without_synchronizing(cached_app, monkeypatch):
    widget, _, _ = cached_app
    widget.computer_selection.value = ("alpha",)
    widget.code_selection.value = ("first",)
    plan = {
        "selection": {"computers": ["alpha"], "codes": ["first"]},
        "codes": {"first-v1@alpha": {"install": True}},
    }
    widget.config = {
        "_metadata": {"config_revision": "reviewed-revision"},
        **deepcopy(widget.catalog),
    }
    widget.updates_needed = deepcopy(plan)
    monkeypatch.setattr(
        start, "check_for_updates", lambda *args, **kwargs: ("", deepcopy(plan))
    )
    monkeypatch.setattr(start, "set_ssh", lambda *args: True)
    monkeypatch.setattr(start, "code_runtimes", lambda *args: [])
    monkeypatch.setattr(start, "execute_setup_tasks", lambda *args: True)
    monkeypatch.setattr(start, "manage_uenv_images", lambda *args: True)
    monkeypatch.setattr(start, "setup_computers", lambda *args: True)
    monkeypatch.setattr(start, "setup_codes", lambda *args: (True, []))
    widget.run_configuration(None)
    assert "configuration applied" in widget.update_message.value
    assert widget.start_button.disabled and not widget.updates_needed


def test_apply_rejects_a_config_pulled_by_another_session(cached_app, monkeypatch):
    widget, _, _ = cached_app
    widget.config = {"_metadata": {"config_revision": "earlier-revision"}}

    def forbidden(*args, **kwargs):
        raise AssertionError("A stale revision must not reach mutation or inspection")

    monkeypatch.setattr(start, "check_for_updates", forbidden)
    widget.run_configuration(None)
    assert "revision changed" in widget.update_message.value
    assert widget.start_button.disabled and not widget.updates_needed


def test_each_selected_remote_has_help_before_authentication_check(
    cached_app, monkeypatch
):
    widget, path, data = cached_app
    updated = deepcopy(data)
    updated["connection_guides"] = {
        "alps": {
            "title": "ALPS SSH",
            "steps": ["Install MFA CSCS and refresh your certificate."],
        },
        "tigu": {
            "title": "Tigu SSH",
            "steps": ["Send the public part of the separate key to SCI-IT."],
        },
    }
    for key, guide in [("alpha", "alps"), ("beta", "tigu")]:
        updated["computers"][key]["setup"]["transport"] = "core.ssh"
        updated["computers"][key]["ssh_help"] = guide

    def sync(**kwargs):
        path.write_text(yaml.safe_dump(updated))
        return True, ""

    monkeypatch.setattr(start, "check_repository", sync)
    monkeypatch.setattr(start, "key_is_valid", lambda **kwargs: False)
    widget.check_for_all_updates(None)
    assert len(widget.connection_help.children) == 2
    assert "MFA CSCS" in widget.connection_help.children[0].value
    assert "SCI-IT" in widget.connection_help.children[1].value
    assert widget.connection_help.selected_index == 0
    assert widget.start_button.disabled
    widget.computer_selection.value = ()
    assert not widget.connection_help.children
