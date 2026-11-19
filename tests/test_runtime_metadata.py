"""Installer identity reaches real Code registration without a private patch."""

import hashlib
import json
import subprocess
import sys

import pytest
from aiida import orm
from aiida.cmdline.commands.cmd_verdi import verdi
from click.testing import CliRunner

from empa_setup_utils import aiida_and_ssh_utils as utils
from empa_setup_utils.runtime_metadata import verified_code_metadata

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.fixture
def runtime(tmp_path):
    executable = tmp_path / "bin/block2main"
    executable.parent.mkdir()
    executable.write_bytes(b"#!/bin/sh\nexit 0\n")
    digest = hashlib.sha256(executable.read_bytes()).hexdigest()
    receipt = {
        "status": "INSTALLED_AND_CHECKED",
        "prefix": str(tmp_path),
        "recipe_sha256": "a" * 64,
        "verified_files": {"bin/block2main": digest},
        "code_extras": {
            "block2": {
                "version": "0.5.4",
                "artifact_sha256": "b" * 64,
                "launcher_sha256": digest,
            }
        },
    }
    path = tmp_path / "empa-runtime.json"
    path.write_text(json.dumps(receipt))
    return {
        "filepath_executable": str(executable),
        "description": "Disposable runtime registration",
        "default_calc_job_plugin": "core.arithmetic.add",
        "runtime_receipt": {
            "path": str(path),
            "recipe_sha256": "a" * 64,
            "extras": {"block2": {"version": "0.5.4", "artifact_sha256": "b" * 64}},
        },
    }


def test_nested_block2_identity_is_registered_through_real_cli(
    runtime, aiida_profile, aiida_localhost, monkeypatch
):
    runner = CliRunner()

    def invoke(command):
        result = runner.invoke(
            verdi, ["--profile", aiida_profile.name, *command[1:]], input=""
        )
        return result.output, result.exit_code == 0

    monkeypatch.setattr(utils, "run_command", invoke)
    label = f"new-native@{aiida_localhost.label}"
    assert utils.setup_aiida_code(label, runtime, install=True)
    stored = orm.load_code(label)
    assert stored.base.extras.get("block2") == verified_code_metadata(runtime)["block2"]
    assert stored.base.extras.get("block2")["version"] == "0.5.4"


@pytest.mark.parametrize("change", ["launcher", "recipe", "version", "status"])
def test_invalid_runtime_fails_before_relabelling_or_any_cli(
    runtime, change, monkeypatch
):
    from pathlib import Path

    path = Path(runtime["runtime_receipt"]["path"])
    receipt = json.loads(path.read_text())
    if change == "launcher":
        Path(runtime["filepath_executable"]).write_bytes(b"changed")
    elif change == "recipe":
        receipt["recipe_sha256"] = "c" * 64
    elif change == "version":
        receipt["code_extras"]["block2"]["version"] = "0.6"
    else:
        receipt["status"] = "FAILED"
    path.write_text(json.dumps(receipt))
    monkeypatch.setattr(
        utils,
        "run_command",
        lambda *a, **kw: pytest.fail("No CLI for unqualified runtime"),
    )
    with pytest.raises(ValueError):
        utils.setup_aiida_code(
            "native@localhost", runtime, hide=True, pktorelabel=123, install=True
        )


def test_installer_logs_stream_without_shell_or_stdin(capsys):
    output, ok = utils.run_installation_command(
        [sys.executable, "-u", "-c", "print('stage-one'); print('stage-two')"],
        timeout=10,
    )
    assert ok and "stage-one" in output and "stage-two" in output
    assert "stage-one" in capsys.readouterr().out


def test_installer_timeout_is_reported(capsys):
    with pytest.raises(subprocess.TimeoutExpired):
        utils.run_installation_command(
            [
                sys.executable,
                "-u",
                "-c",
                "import time; print('started'); time.sleep(10)",
            ],
            timeout=0.5,
        )
    assert "started" in capsys.readouterr().out


def test_local_argument_vector_preserves_spaces(monkeypatch):
    calls = []
    monkeypatch.setattr(
        utils,
        "run_installation_command",
        lambda args, timeout: (calls.append(args) or "ok", True),
    )
    args = ["/python", "/catalog path/install.py", "--prefix", "/runtime path/v1"]
    tasks = [
        {
            "name": "native",
            "computer": "local",
            "commands": [{"type": "local", "arguments": args, "stream": True}],
        }
    ]
    assert utils.execute_setup_tasks(
        tasks, {"computers": {"local": {"setup": {"hostname": "localhost"}}}}
    )
    assert calls == [args]


def test_scalar_capabilities_are_bound_before_registration(runtime):
    from pathlib import Path

    path = Path(runtime["runtime_receipt"]["path"])
    receipt = json.loads(path.read_text())
    receipt["code_extras"] = {
        "supports_spin_graph": True,
        "engine_source_commit": "f" * 40,
    }
    path.write_text(json.dumps(receipt))
    runtime["runtime_receipt"]["extras"] = dict(receipt["code_extras"])
    assert verified_code_metadata(runtime)["supports_spin_graph"] is True
    receipt["code_extras"]["supports_spin_graph"] = 1
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="capability"):
        verified_code_metadata(runtime)


def test_same_verifier_checks_remote_uenv_bytes_before_registration(runtime):
    """Execute the actual stdlib verifier payload, without SSH or uenv control."""
    import shlex

    expected = verified_code_metadata(runtime)
    runtime["runtime_receipt"].update(
        host="daint.alps.cscs.ch",
        image="service::empa-model-native/test:123",
        view="default",
        python=sys.executable,
    )

    def runner(command):
        assert command[:4] == [
            "ssh",
            "-oBatchMode=yes",
            "-oStrictHostKeyChecking=yes",
            "daint.alps.cscs.ch",
        ]
        arguments = shlex.split(command[-1])
        assert arguments[:4] == [
            "uenv",
            "run",
            "--view=default",
            "service::empa-model-native/test:123",
        ]
        process = subprocess.run(arguments[5:], capture_output=True, text=True)
        return process.stdout + process.stderr, process.returncode == 0

    assert verified_code_metadata(runtime, runner) == expected
    from pathlib import Path

    Path(runtime["filepath_executable"]).write_bytes(b"changed remote launcher")
    with pytest.raises(ValueError, match="unavailable or changed"):
        verified_code_metadata(runtime, runner)


def test_model_codes_are_discovered_by_the_real_app(
    runtime, aiida_profile, aiida_localhost, monkeypatch
):
    process = pytest.importorskip("aiidalab_itensor_spectra.process")
    from pathlib import Path

    path = Path(runtime["runtime_receipt"]["path"])
    receipt = json.loads(path.read_text())
    receipt["code_extras"] = {
        "supports_spin_graph": True,
        "supports_spectral_bond_cap": True,
        "supports_blas_execution_policy": True,
    }
    path.write_text(json.dumps(receipt))
    runtime["runtime_receipt"]["extras"] = receipt["code_extras"]
    runner = CliRunner()

    def invoke(command):
        result = runner.invoke(
            verdi, ["--profile", aiida_profile.name, *command[1:]], input=""
        )
        return result.output, result.exit_code == 0

    monkeypatch.setattr(utils, "run_command", invoke)
    suffix = "0.4.3.dev0-local-4f7f2f96"
    for kind, plugin in [("gs", "ground_state"), ("segment", "spectral_segment")]:
        config = {**runtime, "default_calc_job_plugin": "itensor_spin." + plugin}
        assert utils.setup_aiida_code(
            f"itensor-spin-{kind}-{suffix}@{aiida_localhost.label}",
            config,
            install=True,
        )
    pairs = process.find_code_pairs()
    assert len(pairs) == 1 and pairs[0].suffix == suffix
    assert (
        orm.load_code(pairs[0].ground_state_uuid).base.extras.get("supports_spin_graph")
        is True
    )
    assert (
        orm.load_code(pairs[0].spectral_uuid).base.extras.get(
            "supports_spectral_bond_cap"
        )
        is True
    )
