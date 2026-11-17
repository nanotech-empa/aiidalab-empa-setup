"""Unexpected/replaced Codes require explicit choices and retain provenance."""

from copy import deepcopy

import pytest
from aiida import orm
from aiida.cmdline.commands.cmd_verdi import verdi
from aiida.common.links import LinkType
from click.testing import CliRunner

from empa_setup_utils import aiida_and_ssh_utils, control

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.fixture
def context(aiida_profile_clean, aiida_localhost, monkeypatch):
    computer = aiida_localhost
    config = {
        "computers": {
            "local": {
                "setup": {
                    "label": computer.label,
                    "hostname": "localhost",
                    "transport": "core.local",
                }
            }
        },
        "codes": {
            "managed": {
                "computer": "local",
                "label": "worker",
                "filepath_executable": "/bin/false",
                "description": "Reviewed registration",
                "default_calc_job_plugin": "core.arithmetic.add",
                "with_mpi": False,
            }
        },
        "setup_tasks": {},
        "applications": {"app": {"title": "Installed frontend", "codes": ["managed"]}},
        "_automatic_codes": True,
        "_installed_applications": {
            "app": {
                "title": "Installed frontend",
                "paths": ["/app"],
                "distributions": {},
            }
        },
    }
    old = orm.InstalledCode(
        computer=computer,
        label="worker",
        filepath_executable="/bin/true",
        default_calc_job_plugin="core.arithmetic.add",
    ).store()
    calc = orm.CalcJobNode(computer=computer)
    calc.base.links.add_incoming(old, link_type=LinkType.INPUT_CALC, link_label="code")
    calc.store()
    monkeypatch.setattr(control, "check_ssh_config", lambda *a: (True, "", []))
    monkeypatch.setattr(
        control, "compare_computer_configuration", lambda *a: (True, "")
    )
    monkeypatch.setattr(
        control,
        "compare_code_configuration",
        lambda *a: (False, "Existing executable differs.<br>"),
    )
    return config, old.pk, str(old.uuid), calc.pk


def inspect(config, tmp_path, **kwargs):
    ok, message, plan = control.process_aiida_configuration(
        config,
        tmp_path,
        None,
        selected_computers=["local"],
        selected_codes=None,
        **kwargs,
    )
    assert ok, message
    return plan


def test_incompatible_existing_code_is_kept_until_explicit_hide(context, tmp_path):
    config, pk, uuid, calc_pk = context
    original = deepcopy(config)
    code = orm.load_code(pk)
    before = code.base.attributes.all
    plan = inspect(config, tmp_path)
    assert plan["unmanaged"]["codes"] == {code.full_label: pk}
    assert plan["replacements"] == {code.full_label: pk}
    assert any("Keeping " + code.full_label in x for x in plan["blocked"])
    assert not plan.get("codes")
    assert not orm.load_code(pk).is_hidden
    assert orm.load_code(pk).base.attributes.all == before
    assert config == original
    assert str(orm.load_node(calc_pk).inputs.code.uuid) == uuid


def test_explicit_hide_and_required_rename_preserve_old_record_and_links(
    context, tmp_path, monkeypatch
):
    config, pk, uuid, calc_pk = context
    old = orm.load_code(pk)
    full = old.full_label
    before = old.base.attributes.all
    plan = inspect(config, tmp_path, deactivate_codes=[full])
    assert not plan["blocked"]
    assert plan["codes"][full]["rename"] == pk and plan["codes"][full]["hide"]
    assert plan["codes"][full]["install"]
    runner = CliRunner()
    commands = []

    def actual_cli(command, **kwargs):
        commands.append(command)
        assert command[0] == "verdi"
        result = runner.invoke(verdi, command[1:])
        return result.output, result.exit_code == 0

    monkeypatch.setattr(aiida_and_ssh_utils, "run_command", actual_cli)
    success, _ = control.setup_codes(plan["codes"], config)
    assert success
    previous = orm.load_code(pk)
    assert (
        str(previous.uuid) == uuid and previous.is_hidden and previous.label != "worker"
    )
    assert previous.base.attributes.all == before
    assert str(orm.load_node(calc_pk).inputs.code.uuid) == uuid
    current = orm.load_code(full)
    assert current.pk != pk and str(current.filepath_executable) == "/bin/false"
    assert any(x[1:3] == ["code", "hide"] for x in commands)
    assert any(x[1:3] == ["code", "relabel"] for x in commands)
    assert not any("delete" in x for x in commands)


def test_unknown_code_is_hidden_only_if_chosen_without_unnecessary_rename(
    context, tmp_path
):
    config, _, _, _ = context
    custom = orm.InstalledCode(
        computer=orm.load_computer("localhost"),
        label="research-helper",
        filepath_executable="/bin/true",
        default_calc_job_plugin="core.arithmetic.add",
    ).store()
    plan = inspect(config, tmp_path)
    assert custom.full_label in plan["unmanaged"]["codes"]
    assert custom.full_label not in plan.get("codes", {})
    plan = inspect(config, tmp_path, deactivate_codes=[custom.full_label])
    action = plan["codes"][custom.full_label]
    assert action == {
        "hide": True,
        "rename": False,
        "install": False,
        "identifier": custom.pk,
    }
    assert not orm.load_code(custom.pk).is_hidden
