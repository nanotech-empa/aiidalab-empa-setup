"""Remote module identity reaches registration; no remote command executes here."""

import json
from copy import deepcopy

import pytest
from aiida import orm
from aiida.cmdline.commands.cmd_verdi import verdi
from click.testing import CliRunner

from empa_setup_utils import aiida_and_ssh_utils as utils
from empa_setup_utils.runtime_metadata import verified_remote_provider

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.fixture
def provider():
    receipt = {
        "status": "MODULE_LOAD_VERIFIED",
        "family": "block2",
        "prefix": "/remote/stack/block2",
        "module": "empa/block2/0.5.4",
        "module_root": "/remote/stack/modulefiles",
        "recipe_sha256": "a" * 64,
        "module_sha256": "b" * 64,
        "executables": {"bin/block2main": "c" * 64},
        "code_extras": {
            "block2": {
                "version": "0.5.4",
                "artifact_sha256": "d" * 64,
                "launcher_sha256": "c" * 64,
            }
        },
    }
    config = {
        "filepath_executable": "/remote/stack/block2/bin/block2main",
        "description": "Disposable module binding",
        "default_calc_job_plugin": "core.arithmetic.add",
        "runtime_provider": {
            key: receipt[key]
            for key in ["family", "prefix", "module", "module_root", "recipe_sha256"]
        },
    }
    config["runtime_provider"].update(
        host="tigu",
        root="/remote/stack",
        checker="/remote/stack/sources/selected/scripts/deploy-tigu-modules.py",
        checker_sha256="e" * 64,
        extras={"block2": {"version": "0.5.4", "artifact_sha256": "d" * 64}},
    )
    return config, receipt


def test_verified_remote_module_metadata_is_registered(
    provider, aiida_profile, aiida_localhost, monkeypatch
):
    config, receipt = provider
    runner = CliRunner()
    calls = []

    def invoke(arguments):
        calls.append(arguments)
        if arguments[0] == "ssh":
            assert "-oStrictHostKeyChecking=yes" in arguments
            assert "Module checker changed" in arguments[-1]
            return json.dumps(receipt), True
        result = runner.invoke(
            verdi, ["--profile", aiida_profile.name, *arguments[1:]], input=""
        )
        return result.output, result.exit_code == 0

    monkeypatch.setattr(utils, "run_command", invoke)
    label = f"module-block2@{aiida_localhost.label}"
    assert utils.setup_aiida_code(label, config, install=True)
    extras = orm.load_code(label).base.extras.all
    assert extras["block2"] == receipt["code_extras"]["block2"]
    assert extras["empa_module_provider"]["module_sha256"] == "b" * 64
    assert calls[0][0] == "ssh" and calls[1][0] == "verdi"


@pytest.mark.parametrize(
    "field,value",
    [
        ("family", "other"),
        ("prefix", "/another"),
        ("recipe_sha256", "f" * 64),
        ("status", "MISSING"),
    ],
)
def test_changed_provider_stops_before_lifecycle_writes(
    provider, field, value, monkeypatch
):
    config, receipt = provider
    receipt = deepcopy(receipt)
    receipt[field] = value

    def invoke(arguments):
        assert arguments[0] == "ssh", "No relabel/hide/create before qualification"
        return json.dumps(receipt), True

    monkeypatch.setattr(utils, "run_command", invoke)
    with pytest.raises(ValueError, match="receipt differs"):
        utils.setup_aiida_code(
            "native@localhost", config, install=True, hide=True, pktorelabel=123
        )


def test_access_failure_is_not_a_missing_module(provider):
    config, _ = provider
    with pytest.raises(ValueError, match="unavailable or changed"):
        verified_remote_provider(
            config, lambda args: ("Permission denied (publickey)", False)
        )


def test_successful_existing_ela_route_may_append_ssh_information(provider):
    config, receipt = provider
    result = verified_remote_provider(
        config,
        lambda args: (
            json.dumps(receipt)
            + "\nIdentity added: existing route\nLifetime set to 5 seconds",
            True,
        ),
    )
    assert result["block2"] == receipt["code_extras"]["block2"]
    assert result["empa_module_provider"]["module_sha256"] == "b" * 64


def test_model_capabilities_require_exact_booleans(provider):
    config, receipt = provider
    config["runtime_provider"]["extras"] = {"supports_spin_graph": True}
    receipt["code_extras"] = {"supports_spin_graph": 1}
    with pytest.raises(ValueError, match="capability"):
        verified_remote_provider(config, lambda args: (json.dumps(receipt), True))


def test_independent_setup_paths_do_not_change_normal_user_defaults(tmp_path):
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env.update(
        EMPA_SETUP_CONFIG_REPO=str(tmp_path / "catalog"),
        EMPA_SETUP_SSH_DIR=str(tmp_path / "ssh"),
        AIIDA_PATH=str(tmp_path / "isolated-aiida"),
    )
    output = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "from empa_setup_utils import repo_utils as r; import json; print(json.dumps([str(r.GIT_REPO_PATH),str(r.configuration_file),str(r.config_path)]))",
        ],
        env=env,
        text=True,
    )
    assert json.loads(output) == [
        str(tmp_path / "catalog"),
        str(tmp_path / "catalog/config.yml"),
        str(tmp_path / "ssh"),
    ]
    assert not (tmp_path / "catalog").exists() and not (tmp_path / "ssh").exists()
