"""Exercise the actual companion YAML when its reviewed checkout is supplied.

Run with EMPA_SETUP_CATALOG=/path/to/aiidalab-alps-files/config.yml. No native
task, SSH operation or live AiiDA profile is used by these bundle contracts.
"""

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from empa_setup_utils import control
from empa_setup_utils.selection import (
    installation_tasks,
    select_resources,
    selected_ssh_config,
)

CATALOG = os.environ.get("EMPA_SETUP_CATALOG")
pytestmark = pytest.mark.skipif(
    not CATALOG,
    reason="Supply the exact companion catalog checkout for paired-source tests",
)


def rendered(monkeypatch, **kwargs):
    # Parse the ARM64 catalog on any CI host; no native task executes here.
    monkeypatch.setattr(
        "empa_setup_utils.selection.platform.machine", lambda: "aarch64"
    )
    monkeypatch.setattr(
        control, "get_local_commit", lambda: "reviewed-fixture-revision"
    )
    ok, message, config = control.get_config(
        file_path=Path(CATALOG),
        deployment="personal",
        selected_computers=["mac-localhost"],
        config_widgets={"cscsuser": SimpleNamespace(value="select")},
        **kwargs,
    )
    assert ok, message
    return config


@pytest.mark.parametrize(
    "codes,expected",
    [
        (["local_pythonjob"], []),
        (["local_banduppy"], ["local_banduppy_runtime"]),
        (
            ["local_cp2k", "local_stm", "local_sparse_overlap"],
            ["local_cp2k_spm_runtime"],
        ),
        (["local_qe_pw", "local_qe_pp"], ["local_qe75_runtime"]),
        (["local_block2"], ["local_block2_runtime"]),
        (
            ["local_cubehandler", "local_orca_cube_render"],
            ["local_cube_render_runtime"],
        ),
        (
            ["local_itensor_gs", "local_itensor_segment"],
            ["local_julia_runtime", "local_itensor_project"],
        ),
        (["local_orca", "local_orca_plot"], ["local_orca_runtime"]),
        (["local_bader", "local_ppafm", "local_critic2"], ["local_native_support"]),
    ],
)
def test_selected_real_catalog_has_only_its_own_tasks(monkeypatch, codes, expected):
    config = rendered(monkeypatch)
    computers, codes = select_resources(config, None, ["mac-localhost"], codes)
    tasks = installation_tasks(config, computers, codes)
    assert [t["name"] for t in tasks] == expected
    assert selected_ssh_config(config, computers) == {}
    for code in codes:
        assert config["codes"][code]["filepath_executable"].startswith("/")
        assert "{" not in config["codes"][code]["filepath_executable"]


def test_existing_programs_mode_skips_only_local_installers(monkeypatch):
    config = rendered(monkeypatch, install_local_runtimes=False)
    assert (
        installation_tasks(config, {"mac-localhost"}, {"local_cp2k", "local_block2"})
        == []
    )


@pytest.mark.parametrize("old_schema", [2, 3])
def test_older_consumer_refuses_new_protocol_before_apply(monkeypatch, old_schema):
    monkeypatch.setattr(control, "SUPPORTED_SCHEMA_VERSION", old_schema)
    ok, message, config = control.get_config(file_path=CATALOG, deployment="personal")
    assert not ok and f"schema {old_schema}" in message and "schema 4" in message
    assert config == {}


def test_exact_local_resources_and_paths_are_rendered(monkeypatch):
    config = rendered(
        monkeypatch, settings={"local_default_mpi": "4", "local_memory_kib": "12582912"}
    )
    setup = config["computers"]["mac-localhost"]["setup"]
    assert setup["mpiprocs_per_machine"] == "4"
    assert setup["default_memory_per_machine"] == "12582912"
    assert setup["transport"] == "core.local" and setup["scheduler"] == "core.direct"
    assert "2026.2" in config["codes"]["local_cp2k"]["filepath_executable"]
    assert "7.5" in config["codes"]["local_qe_pw"]["filepath_executable"]


@pytest.mark.parametrize(
    "setting,value",
    [
        ("local_default_mpi", "0"),
        ("local_memory_kib", "oops"),
        ("local_qe_prefix", "/tmp/$(whoami)"),
    ],
)
def test_invalid_paths_and_resources_fail_at_inspection(monkeypatch, setting, value):
    ok, _, config = control.get_config(
        file_path=CATALOG,
        deployment="personal",
        selected_computers=["mac-localhost"],
        settings={setting: value},
    )
    assert not ok and config == {}


@pytest.mark.parametrize(
    "code,family",
    [
        ("tigu_module_banduppy", "banduppy"),
        ("tigu_module_cp2k", "cp2k"),
        ("tigu_module_block2", "block2"),
        ("tigu_module_itensor_gs", "model"),
        ("tigu_module_phonopy", "support"),
        ("tigu_module_orca", "orca"),
    ],
)
def test_real_module_catalog_is_scoped_and_has_no_local_paths(
    monkeypatch, code, family
):
    monkeypatch.setattr(
        control, "get_local_commit", lambda: "reviewed-fixture-revision"
    )
    ok, message, config = control.get_config(
        file_path=CATALOG,
        deployment="hosted",
        selected_computers=["tigu-modules"],
        computer_grants={"tigu-modules": "free"},
        config_widgets={"cscsuser": SimpleNamespace(value="cpi")},
    )
    assert ok, message
    computers, codes = select_resources(config, "free", ["tigu-modules"], [code])
    tasks = installation_tasks(config, computers, codes)
    assert [task["name"] for task in tasks] == ["tigu_modules_" + family]
    selected = config["codes"][code]
    assert selected["computer"] == "tigu-modules"
    assert selected["runtime_provider"]["family"] == family
    assert "module load empa/" in selected["prepend_text"]
    for path in [
        selected["filepath_executable"],
        selected["runtime_provider"]["checker"],
        selected["runtime_provider"]["root"],
    ]:
        assert "{" not in path and not path.startswith("/home/jovyan")
    assert (
        config["computers"]["tigu-modules"]["setup"]["mpirun_command"]
        == "mpirun -np {tot_num_mpiprocs}"
    )
    assert set(selected_ssh_config(config, computers)) <= {"tigu", "ela.cscs.ch"}
