"""Exercise the actual widget -> planner -> AiiDA CLI localhost setup route."""

import yaml
from aiida import orm
from aiida.cmdline.commands.cmd_verdi import verdi
from click.testing import CliRunner

import start
from empa_setup_utils import aiida_and_ssh_utils, control

pytest_plugins = ["aiida.tools.pytest_fixtures"]


def test_running_work_guard_checks_only_affected_computers(
    aiida_profile, aiida_localhost
):
    from aiida.common.links import LinkType
    from plumpy import ProcessState

    code = orm.InstalledCode(
        label="active-test", computer=aiida_localhost, filepath_executable="/bin/true"
    ).store()
    node = orm.CalcJobNode(computer=aiida_localhost)
    node.base.links.add_incoming(code, link_type=LinkType.INPUT_CALC, link_label="code")
    node.set_process_state(ProcessState.WAITING)
    node.store()
    identifier = str(node.uuid)
    assert aiida_and_ssh_utils.unfinished_processes_for_computers(
        {aiida_localhost.label}
    ) == (True, identifier)
    assert aiida_and_ssh_utils.unfinished_processes_for_computers({"unrelated"}) == (
        False,
        "",
    )


def test_personal_configuration_creates_local_computer_and_code_without_ssh(
    aiida_profile, tmp_path, monkeypatch
):
    config = {
        "schema_version": 2,
        "computers": {
            "mac-localhost": {
                "deployments": ["personal"],
                "setup": {
                    "label": "personal-test",
                    "hostname": "localhost",
                    "description": "Disposable personal computer",
                    "transport": "core.local",
                    "scheduler": "core.direct",
                    "shebang": "#!/bin/bash",
                    "work_dir": str(tmp_path / "work"),
                    "mpirun_command": "mpirun -np {tot_num_mpiprocs}",
                    "mpiprocs_per_machine": 1,
                    "default_memory_per_machine": 1048576,
                    "prepend_text": "",
                    "use_double_quotes": False,
                },
                "config": {},
            }
        },
        "codes": {
            "true": {
                "computer": "mac-localhost",
                "label": "true",
                "description": "Disposable executable",
                "filepath_executable": "/bin/true",
                "default_calc_job_plugin": "core.arithmetic.add",
                "with_mpi": False,
            }
        },
        "setup_tasks": {},
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(config))
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(start, "available_config_branches", lambda **kwargs: ["main"])
    monkeypatch.setattr(start, "check_repository", lambda **kwargs: (True, ""))
    monkeypatch.setattr(control, "check_repository", lambda **kwargs: (True, ""))
    monkeypatch.setattr(control, "get_local_commit", lambda: "test-revision")
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp, "check_paused_workchains", lambda self: (False, "")
    )
    original = start.ConfigAiiDAlabApp.widgets_from_yaml
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp,
        "widgets_from_yaml",
        lambda self, **kwargs: original(self, **{**kwargs, "file_path": path}),
    )
    monkeypatch.setattr(
        start,
        "get_config",
        lambda **kwargs: control.get_config(**{**kwargs, "file_path": path}),
    )
    monkeypatch.setattr(control, "config_path", tmp_path / "ssh")
    monkeypatch.setattr(start, "config_path", tmp_path / "ssh")
    runner = CliRunner()
    commands = []

    def invoke(command):
        assert command[0] == "verdi", (
            "No SSH or installation command belongs in local registration"
        )
        commands.append(command)
        result = runner.invoke(
            verdi, ["--profile", aiida_profile.name, *command[1:]], input=""
        )
        return result.output, result.exit_code == 0

    monkeypatch.setattr(aiida_and_ssh_utils, "run_command", invoke)
    widget = start.ConfigAiiDAlabApp()
    try:
        widget.check_for_all_updates(None)
        assert not widget.start_button.disabled, widget.update_message.value
        widget.run_configuration(None)
        assert "applied" in widget.update_message.value, widget.update_message.value
        computer = orm.load_computer("personal-test")
        code = orm.load_code("true@personal-test")
        assert computer.transport_type == "core.local"
        assert computer.is_user_configured(orm.User.collection.get_default())
        assert code.with_mpi is False
        assert str(code.filepath_executable) == "/bin/true"
        assert not (tmp_path / "ssh").exists()
        # Hosted mode removes local resources and invalidates the personal plan.
        widget.deployment.value = "hosted"
        assert (
            not widget.computer_selection.options and not widget.code_selection.options
        )
        assert widget.start_button.disabled
        assert any(
            args[1:4] == ["computer", "configure", "core.local"] for args in commands
        )
    finally:
        widget.close()
