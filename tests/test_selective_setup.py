"""User-visible safety and selection scenarios, without a live profile or SSH."""

from copy import deepcopy

import pytest

from empa_setup_utils import control
from empa_setup_utils.configuration import load_config_bundle
from empa_setup_utils.selection import installation_tasks, select_resources


@pytest.fixture
def inspected(monkeypatch):
    config = {
        "computers": {
            "daint": {"grants": ["first", "second"], "setup": {"label": "daint_first"}},
            "tigu": {"grants": ["free"], "setup": {"label": "tigu_free"}},
        },
        "codes": {
            "cp2k": {"computer": "daint", "label": "cp2k"},
            "python": {"computer": "daint", "label": "python"},
            "orca": {"computer": "tigu", "label": "orca"},
        },
        "setup_tasks": {},
    }
    monkeypatch.setattr(control, "check_ssh_config", lambda *a: (True, "", []))
    monkeypatch.setattr(
        control,
        "aiida_computers",
        lambda: (True, "", {"daint_first", "tigu_free", "research"}, set()),
    )
    monkeypatch.setattr(
        control,
        "aiida_codes",
        lambda: (
            True,
            "",
            {
                ("cp2k", "daint_first", 1),
                ("custom", "daint_first", 2),
                ("worker", "research", 3),
            },
            set(),
        ),
    )
    monkeypatch.setattr(
        control, "compare_computer_configuration", lambda *a: (True, "")
    )
    monkeypatch.setattr(control, "compare_code_configuration", lambda *a: (True, ""))
    return config


def test_unlisted_resources_are_kept_and_reported(inspected, tmp_path):
    original = deepcopy(inspected)
    ok, _, plan = control.process_aiida_configuration(
        inspected,
        tmp_path,
        "first",
        selected_computers=["daint"],
        selected_codes=["cp2k"],
    )
    assert ok
    assert plan["unmanaged"] == {
        "computers": ["research"],
        "codes": {"custom@daint_first": 2, "worker@research": 3},
    }
    assert not plan.get("computers")
    assert set(plan["codes"]) == {"cp2k@daint_first"}
    assert inspected == original


def test_deactivation_requires_explicit_choices_and_does_not_rename(
    inspected, tmp_path
):
    ok, _, plan = control.process_aiida_configuration(
        inspected,
        tmp_path,
        "first",
        selected_computers=[],
        selected_codes=[],
        deactivate_computers=["research"],
        deactivate_codes=["custom@daint_first"],
    )
    assert ok
    assert plan["computers"] == {
        "research": {"hide": True, "rename": False, "install": False}
    }
    assert plan["codes"]["custom@daint_first"] == {
        "hide": True,
        "rename": False,
        "install": False,
        "identifier": 2,
    }
    assert "worker@research" not in plan["codes"]


def test_differences_on_unselected_computers_are_never_examined(
    inspected, tmp_path, monkeypatch
):
    checked = []
    monkeypatch.setattr(
        control,
        "compare_computer_configuration",
        lambda name, data: (checked.append(name) or False, "different workdir"),
    )
    ok, _, plan = control.process_aiida_configuration(
        inspected,
        tmp_path,
        "first",
        selected_computers=["daint"],
        selected_codes=[],
        update_computers=True,
    )
    assert ok and checked == ["daint_first"]
    assert set(plan["computers"]) == {"daint_first"}
    assert plan["blocked"] and "custom@daint_first" in plan["blocked"][0]


def test_adding_a_code_can_keep_a_working_computer_definition(
    inspected, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        control,
        "compare_computer_configuration",
        lambda *a: (False, "different memory"),
    )
    ok, _, plan = control.process_aiida_configuration(
        inspected,
        tmp_path,
        "first",
        selected_computers=["daint"],
        selected_codes=["python"],
    )
    assert ok and not plan.get("computers") and not plan["blocked"]
    assert plan["codes"]["python@daint_first"]["install"]


def test_multiple_computer_accounts_and_deployment_profiles():
    config = {
        "_deployment": "hosted",
        "_computer_grants": {"daint": "first", "tigu": "free"},
        "computers": {
            "daint": {"grants": ["first"]},
            "tigu": {"grants": ["free"]},
            "local": {"deployments": ["personal"]},
        },
        "codes": {},
    }
    assert select_resources(config, None)[0] == {"daint", "tigu"}
    with pytest.raises(ValueError, match="chosen grant"):
        select_resources(config, None, ["local"])
    config["_deployment"] = "personal"
    assert select_resources(config, None, ["local"], []) == ({"local"}, set())


def test_only_selected_code_installation_tasks_run():
    config = {
        "computers": {"daint": {}},
        "codes": {"cp2k": {"computer": "daint"}, "bader": {"computer": "daint"}},
        "setup_tasks": {
            "shared": {"computer": "daint"},
            "cp2k_data": {"computer": "daint", "codes": ["cp2k"]},
            "bader_build": {"computer": "daint", "codes": ["bader"]},
        },
    }
    original = deepcopy(config)
    assert [
        task["name"] for task in installation_tasks(config, {"daint"}, {"cp2k"})
    ] == ["shared", "cp2k_data"]
    assert config == original


def test_partial_selection_rejects_unscoped_legacy_commands():
    with pytest.raises(ValueError, match="unscoped"):
        installation_tasks(
            {
                "computers": {"daint": {}, "tigu": {}},
                "codes": {},
                "custom_commands": {"dangerous": []},
            },
            {"tigu"},
            set(),
        )


def test_config_includes_reject_duplicates_and_parent_traversal(tmp_path):
    path = tmp_path / "config.yml"
    path.write_text("includes: [extra.yml]\ncomputers: {daint: {}}\n")
    (tmp_path / "extra.yml").write_text("computers: {daint: {}}\n")
    with pytest.raises(ValueError, match="Duplicate"):
        load_config_bundle(path)
    path.write_text("includes: [../private.yml]\n")
    with pytest.raises(ValueError, match="inside"):
        load_config_bundle(path)
