"""Real isolated CLI registration; network commands are independently audited."""

from copy import deepcopy

from aiida import orm
from aiida.cmdline.commands.cmd_verdi import verdi
from click.testing import CliRunner

from empa_setup_utils import aiida_and_ssh_utils as utils

pytest_plugins = ["aiida.tools.pytest_fixtures"]


def test_openssh_computer_configured_with_actual_cli(
    aiida_profile, monkeypatch, tmp_path
):
    config = {
        "setup": {
            "hostname": "example.invalid",
            "description": "Disposable OpenSSH configuration",
            "transport": "core.ssh_async",
            "scheduler": "core.slurm",
            "shebang": "#!/bin/bash",
            "work_dir": str(tmp_path),
            "mpirun_command": "mpirun -np {tot_num_mpiprocs}",
            "mpiprocs_per_machine": 4,
            "default_memory_per_machine": 1024,
            "prepend_text": "",
            "use_double_quotes": False,
        },
        "config": {"host": "tigu", "backend": "openssh", "safe_interval": 30.0},
    }
    before = deepcopy(config)
    calls = []
    runner = CliRunner()

    def invoke(command):
        calls.append(command)
        result = runner.invoke(
            verdi, ["--profile", aiida_profile.name, *command[1:]], input=""
        )
        return result.output, result.exit_code == 0

    monkeypatch.setattr(utils, "run_command", invoke)
    assert utils.setup_aiida_computer("isolated-openssh", config, install=True)
    computer = orm.load_computer("isolated-openssh")
    assert computer.transport_type == "core.ssh_async"
    auth = computer.get_authinfo(orm.User.collection.get_default()).get_auth_params()
    assert auth["backend"] == "openssh" and auth["host"] == "tigu"
    assert auth.get("use_sftp", True) is True and auth["safe_interval"] == 30
    assert config == before
    assert not any("--key-policy" in call or "--allow-agent" in call for call in calls)
    assert utils.compare_computer_configuration("isolated-openssh", config)[0]


def test_async_route_checks_existing_trust_without_keyscan(monkeypatch):
    calls = []
    monkeypatch.setattr(
        utils, "run_command", lambda args: (calls.append(args) or "", True)
    )
    config = {
        "tigu": {
            "ssh_host": "tigu",
            "setup": {"transport": "core.ssh_async", "hostname": "example.invalid"},
            "config": {"host": "tigu"},
        }
    }
    assert utils.set_ssh(config, ["tigu"])
    assert calls == [["ssh", "tigu", "true"]]


def test_async_proxy_change_remains_visible_to_inspect(tmp_path):
    old = {"tigu": {"hostname": "example.invalid", "proxy_command": "ssh old-proxy"}}
    utils.update_ssh_config(tmp_path, old)
    computers = {
        "selected": {
            "ssh_host": "tigu",
            "setup": {"hostname": "example.invalid"},
            "config": {"host": "tigu"},
        }
    }
    new = {"tigu": {**old["tigu"], "proxy_command": "ssh new-proxy"}}
    assert not utils.check_ssh_config(tmp_path, computers, new)[0]
    utils.update_ssh_config(tmp_path, new)
    assert utils.check_ssh_config(tmp_path, computers, new)[0]
    assert "StrictHostKeyChecking yes" in (tmp_path / "config").read_text()
