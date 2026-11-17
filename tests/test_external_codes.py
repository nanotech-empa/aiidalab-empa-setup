"""Inspect the real companion catalog and isolated registrations, without installs."""

import json
import os
from copy import deepcopy
from pathlib import Path

import pytest
from aiida import orm

from empa_setup_utils import control
from empa_setup_utils.configuration import load_config_bundle

pytest_plugins = ["aiida.tools.pytest_fixtures"]
CATALOG = os.environ.get("EMPA_SETUP_CATALOG")
pytestmark = pytest.mark.skipif(not CATALOG, reason="Supply the companion catalog")


@pytest.fixture
def local_catalog(aiida_profile_clean, aiida_localhost, monkeypatch):
    config = load_config_bundle(Path(CATALOG))
    config["_deployment"] = "personal"
    config["computers"]["mac-localhost"]["setup"]["label"] = aiida_localhost.label
    monkeypatch.setattr(control, "check_ssh_config", lambda *a: (True, "", []))
    monkeypatch.setattr(
        control, "compare_computer_configuration", lambda *a: (True, "")
    )
    return config, aiida_localhost


def inspect(config, tmp_path, **kwargs):
    return control.process_aiida_configuration(
        config,
        tmp_path,
        None,
        selected_computers=kwargs.pop("selected_computers", ["mac-localhost"]),
        selected_codes=[],
        **kwargs,
    )


def make_code(computer, label, plugin):
    return orm.InstalledCode(
        label=label,
        computer=computer,
        filepath_executable="/existing/bin/program",
        default_calc_job_plugin=plugin,
    ).store()


def snapshot():
    return [
        (
            node.pk,
            str(node.uuid),
            node.label,
            node.base.attributes.all,
            node.base.extras.all,
            node.base.repository.list_object_names(),
        )
        for node in orm.QueryBuilder().append(orm.Node).all(flat=True)
    ]


def test_all_qe_and_qe_pp_registrations_are_kept_without_actions(
    local_catalog, tmp_path
):
    config, computer = local_catalog
    for name, plugin in config["external_code_providers"]["qe_localhost"][
        "codes"
    ].items():
        make_code(computer, f"{name}-7.5", plugin)
    make_code(computer, "python3", "pythonjob.pythonjob")
    make_code(computer, "critic2", "critic2")
    before, original = snapshot(), deepcopy(config)
    ok, message, plan = inspect(config, tmp_path)
    assert ok and not plan["blocked"]
    assert not plan["unmanaged"]["codes"]
    assert not plan.get("codes") and not plan.get("computers") and not plan["tasks"]
    providers = plan["external_code_providers"]
    assert len(providers["qe_localhost"]["present"]) == 15
    assert not providers["qe_localhost"]["missing"]
    assert len(providers["qe_pp_localhost"]["present"]) == 2
    assert "kept unchanged" in message and "not been probed" in message
    assert "install the Quantum ESPRESSO app" not in message
    json.dumps(plan)  # No live ORM objects survive inspection.
    assert snapshot() == before and config == original


def test_missing_qe_is_optional_and_points_to_its_app(local_catalog, tmp_path):
    config, _ = local_catalog
    ok, message, plan = inspect(config, tmp_path)
    assert ok and not plan["blocked"] and not plan["tasks"] and not plan.get("codes")
    assert len(plan["external_code_providers"]["qe_localhost"]["missing"]) == 15
    assert (
        "If you want QE codes on localhost, install the Quantum ESPRESSO app" in message
    )
    assert "qe_pp_localhost" not in plan["external_code_providers"]
    ok, message, plan = inspect(config, tmp_path, selected_computers=[])
    assert ok and not plan["external_code_providers"]
    assert "install the Quantum ESPRESSO app" not in message


def test_partial_hidden_and_custom_alias_are_reported_honestly(local_catalog, tmp_path):
    config, computer = local_catalog
    alias = make_code(computer, "my-working-pw", "quantumespresso.pw")
    hidden = make_code(computer, "ph-7.5", "quantumespresso.ph")
    hidden.is_hidden = True
    before = snapshot()
    ok, message, plan = inspect(config, tmp_path)
    assert ok
    provider = plan["external_code_providers"]["qe_localhost"]
    assert provider["present"] == {alias.full_label: alias.pk}
    assert provider["unavailable"] == {hidden.full_label: hidden.pk}
    assert "pw" not in provider["missing"] and "ph" in provider["missing"]
    assert "not counted as available" in message
    assert not plan["unmanaged"]["codes"] and snapshot() == before


def test_disabled_local_computer_does_not_claim_available(local_catalog, tmp_path):
    config, computer = local_catalog
    code = make_code(computer, "pw-7.5", "quantumespresso.pw")
    computer.configure()
    computer.get_authinfo(orm.User.collection.get_default()).enabled = False
    ok, _, plan = inspect(config, tmp_path, selected_computers=[])
    assert ok
    provider = plan["external_code_providers"]["qe_localhost"]
    assert not provider["present"]
    assert provider["unavailable"] == {code.full_label: code.pk}
    assert len(provider["missing"]) == 15


def test_wrong_plugin_and_remote_code_are_not_recognized_by_label(
    local_catalog, tmp_path
):
    config, computer = local_catalog
    wrong = make_code(computer, "pw-7.5", "core.arithmetic.add")
    remote = orm.Computer(
        label="remote",
        hostname="example.invalid",
        transport_type="core.ssh",
        scheduler_type="core.direct",
        workdir="/tmp/test",
    ).store()
    remote.configure()
    other = make_code(remote, "pw-7.5", "quantumespresso.pw")
    before = snapshot()
    ok, _, plan = inspect(config, tmp_path)
    assert ok
    assert plan["unmanaged"]["codes"] == {
        wrong.full_label: wrong.pk,
        other.full_label: other.pk,
    }
    assert not plan["external_code_providers"]["qe_localhost"]["present"]
    assert snapshot() == before
    # A local-looking Computer label must not hide a remote transport.
    spoofed = make_code(computer, "my-pw", "quantumespresso.pw")
    computer.transport_type = "core.ssh"
    ok, _, plan = inspect(config, tmp_path)
    assert ok and spoofed.full_label in plan["unmanaged"]["codes"]


def test_stale_deactivation_choice_cannot_hide_a_recognized_code(
    local_catalog, tmp_path
):
    config, computer = local_catalog
    code = make_code(computer, "pw-7.5", "quantumespresso.pw")
    before = snapshot()
    ok, message, plan = inspect(config, tmp_path, deactivate_codes=[code.full_label])
    assert not ok and "code choices changed" in message and not plan
    assert snapshot() == before


def test_managed_helper_remains_under_its_existing_runtime_checks(
    local_catalog, tmp_path
):
    config, computer = local_catalog
    data = config["codes"]["local_critic2"]
    code = make_code(computer, data["label"], data["default_calc_job_plugin"])
    ok, _, plan = inspect(config, tmp_path)
    assert ok and "qe_pp_localhost" not in plan["external_code_providers"]
    assert code.full_label not in plan["unmanaged"]["codes"]
