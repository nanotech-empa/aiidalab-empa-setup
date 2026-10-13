"""Exercise code registration through the real, non-interactive AiiDA CLI."""

from unittest.mock import patch

import pytest
from aiida import orm
from aiida.cmdline.commands.cmd_verdi import verdi
from click.testing import CliRunner

from empa_setup_utils.aiida_and_ssh_utils import setup_aiida_code

pytest_plugins = ["aiida.tools.pytest_fixtures"]


@pytest.mark.parametrize("use_double_quotes", [False, True])
def test_code_registration_without_stdin(aiida_profile, aiida_localhost, use_double_quotes):
    config = {
        "filepath_executable": "/bin/true",
        "description": "Preview registration regression",
        "default_calc_job_plugin": "core.arithmetic.add",
        "prepend_text": "export EXAMPLE=1",
        "append_text": " ",
        "use_double_quotes": use_double_quotes,
    }
    runner = CliRunner()

    def invoke(command):
        result = runner.invoke(verdi, ["--profile", aiida_profile.name, *command[1:]], input="")
        return result.output, result.exit_code == 0

    with patch("empa_setup_utils.aiida_and_ssh_utils.run_command", side_effect=invoke):
        assert setup_aiida_code(f"preview-{use_double_quotes}@{aiida_localhost.label}", config, install=True)

    code = orm.load_code(f"preview-{use_double_quotes}@{aiida_localhost.label}")
    assert str(code.filepath_executable) == "/bin/true"
    assert code.default_calc_job_plugin == config["default_calc_job_plugin"]
    assert code.prepend_text == config["prepend_text"]
    assert code.use_double_quotes == use_double_quotes
