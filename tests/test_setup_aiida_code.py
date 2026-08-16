from unittest.mock import patch

from empa_setup_utils.aiida_and_ssh_utils import setup_aiida_code


def test_code_creation_is_non_interactive():
    config = {
        "filepath_executable": "/remote/bin/example",
        "description": "Example code",
        "default_calc_job_plugin": "example.plugin",
        "prepend_text": "export EXAMPLE=1",
        "append_text": " ",
        "use_double_quotes": False,
    }

    with patch(
        "empa_setup_utils.aiida_and_ssh_utils.run_command",
        return_value=("", True),
    ) as run_command:
        assert setup_aiida_code("example@daint.alps_lp83", config, install=True)

    command = run_command.call_args.args[0]
    assert command[:4] == ["verdi", "code", "create", "core.code.installed"]
    assert "--non-interactive" in command
