import subprocess

from empa_setup_utils import aiida_and_ssh_utils as utils


def test_batch_commands_never_read_passwords_from_notebook_stdin(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, "ok", "")

    monkeypatch.setattr(utils.subprocess, "run", run)
    assert utils.run_command(["ssh", "daint.alps.cscs.ch", "true"]) == ("ok", True)
    command, kwargs = calls[0]
    assert "-oBatchMode=yes" in command
    assert "-oStrictHostKeyChecking=yes" in command
    assert kwargs["stdin"] == subprocess.DEVNULL
    assert kwargs["timeout"]


def test_ssh_updates_preserve_unrelated_entries_and_are_repeatable(tmp_path):
    path = tmp_path / "config"
    original = "Host my-project\n  HostName private.example\n  User scientist\n"
    path.write_text(original)
    selected = {
        "daint.alps.cscs.ch": {"hostname": "daint.alps.cscs.ch", "user": "tester"}
    }
    utils.update_ssh_config(tmp_path, selected)
    once = path.read_text()
    assert original in once
    assert "BatchMode yes" in once and "StrictHostKeyChecking yes" in once
    assert len(list(tmp_path.glob("config.backup-*"))) == 1
    utils.update_ssh_config(tmp_path, selected)
    assert path.read_text() == once
    assert len(list(tmp_path.glob("config.backup-*"))) == 1
    assert path.stat().st_mode & 0o777 == 0o600


def test_changed_trusted_host_key_is_rejected(tmp_path, monkeypatch):
    directory = tmp_path / ".ssh"
    directory.mkdir()
    path = directory / "known_hosts"
    original = "target ssh-ed25519 OLDKEY\n"
    path.write_text(original)
    monkeypatch.setattr(
        utils.os.path,
        "expanduser",
        lambda value: str(tmp_path / value.removeprefix("~/")),
    )
    monkeypatch.setattr(
        utils,
        "run_command",
        lambda args: (
            original if args[0] == "ssh-keygen" else "target ssh-ed25519 NEWKEY",
            True,
        ),
    )
    assert not utils.add_to_known_hosts(["ssh-keyscan", "target"])
    assert path.read_text() == original


def test_missing_certificate_is_actionable_not_a_password_retry_loop():
    message = utils.ssh_failure_message(
        "daint.alps.cscs.ch", "Permission denied (publickey)"
    )
    assert "MFA" in message and "agent" in message
