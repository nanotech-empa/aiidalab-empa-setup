"""Exercise key preparation only in disposable directories, never on ELA."""

import stat
import subprocess
from types import SimpleNamespace

import pytest
import yaml

from empa_setup_utils import control, ssh_onboarding


def run_key_script(directory):
    return subprocess.run(
        ["bash", "-c", ssh_onboarding.REMOTE_KEY_SCRIPT, "--", str(directory)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=15,
    )


def test_key_creation_is_private_idempotent_and_returns_only_public_key(tmp_path):
    directory = tmp_path / "keys"
    first = run_key_script(directory)
    assert first.returncode == 0, first.stderr
    assert first.stdout.startswith("ssh-ed25519 ")
    assert len(first.stdout.splitlines()) == 1
    private = directory / "id_ed25519"
    original = private.read_bytes()
    assert stat.S_IMODE(private.stat().st_mode) == 0o600
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert run_key_script(directory).stdout == first.stdout
    assert private.read_bytes() == original


def test_incomplete_key_pair_is_not_overwritten(tmp_path):
    (tmp_path / "id_ed25519").write_text("existing private file")
    result = run_key_script(tmp_path)
    assert result.returncode != 0 and "nothing was overwritten" in result.stderr
    assert (tmp_path / "id_ed25519").read_text() == "existing private file"


def test_symlink_key_directory_is_refused(tmp_path):
    target = tmp_path / "existing"
    target.mkdir()
    link = tmp_path / "keys"
    link.symlink_to(target)
    assert run_key_script(link).returncode != 0
    assert not list(target.iterdir())


def test_mismatched_public_file_is_refused(tmp_path):
    first, other = tmp_path / "first", tmp_path / "other"
    assert run_key_script(first).returncode == 0
    assert run_key_script(other).returncode == 0
    private = (first / "id_ed25519").read_bytes()
    (first / "id_ed25519.pub").write_bytes((other / "id_ed25519.pub").read_bytes())
    result = run_key_script(first)
    assert result.returncode != 0 and "fingerprints differ" in result.stderr
    assert (first / "id_ed25519").read_bytes() == private


def test_preparation_wrapper_verifies_public_output_and_disables_forwarding(
    tmp_path, monkeypatch
):
    public = run_key_script(tmp_path / "keys").stdout.strip()
    monkeypatch.setattr(ssh_onboarding, "add_to_known_hosts", lambda args: True)
    calls = []

    def remote(args):
        calls.append(args)
        return public, True

    monkeypatch.setattr(ssh_onboarding, "run_command", remote)
    assert ssh_onboarding.prepare_ela_key("testuser") == public
    assert "-oForwardAgent=no" in calls[0]
    assert calls[0][calls[0].index("-l") + 1] == "testuser"
    with pytest.raises(ValueError):
        ssh_onboarding.prepare_ela_key("user; bad")
    assert len(calls) == 1
    monkeypatch.setattr(
        ssh_onboarding, "run_command", lambda args: ("ssh-ed25519 invalid", True)
    )
    with pytest.raises(RuntimeError, match="invalid"):
        ssh_onboarding.prepare_ela_key("testuser")


@pytest.fixture
def onboarding_config(tmp_path):
    data = {
        "variables": {
            "tigu_empa_username": "{cscsuser}",
            "tigu_ela_key": "",
            "python": "/store/{tigu_empa_username}/python",
        },
        "widgets": {"cscsuser": ["select", "cscsaccount"]},
        "runtime_settings": {
            "tigu_empa_username": {"format": "ssh_username", "computers": ["tigu"]},
            "tigu_ela_key": {"format": "ssh_key_path", "computers": ["tigu"]},
        },
        "computers": {
            "tigu": {
                "config": {
                    "username": "{tigu_empa_username}",
                    "proxy_command": "ssh-add -t 5 {tigu_ela_key} && nc jump1.empa.ch 22",
                }
            }
        },
        "codes": {"python": {"filepath_executable": "{python}"}},
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_config_preserves_default_route_and_renders_selected_key_and_account(
    onboarding_config,
):
    args = {
        "file_path": onboarding_config,
        "config_widgets": {"cscsuser": SimpleNamespace(value="cscsaccount")},
        "selected_computers": ["tigu"],
    }
    ok, message, config = control.get_config(**args)
    assert ok, message
    assert config["computers"]["tigu"]["config"]["username"] == "cscsaccount"
    assert (
        config["computers"]["tigu"]["config"]["proxy_command"]
        == "ssh-add -t 5  && nc jump1.empa.ch 22"
    )
    ok, message, config = control.get_config(
        **args,
        settings={
            "tigu_empa_username": "empaaccount",
            "tigu_ela_key": ssh_onboarding.ELA_KEY_PATH,
        },
    )
    assert ok, message
    assert config["computers"]["tigu"]["config"]["username"] == "empaaccount"
    assert (
        ssh_onboarding.ELA_KEY_PATH
        in config["computers"]["tigu"]["config"]["proxy_command"]
    )
    assert (
        config["codes"]["python"]["filepath_executable"] == "/store/empaaccount/python"
    )


@pytest.mark.parametrize(
    "settings",
    [
        {"tigu_empa_username": "user;echo bad"},
        {"tigu_ela_key": "~/key' && echo bad"},
        {"tigu_ela_key": "/tmp/$(echo bad)"},
        {"tigu_ela_key": "-option"},
    ],
)
def test_proxy_settings_reject_shell_input(onboarding_config, settings):
    ok, _, _ = control.get_config(
        file_path=onboarding_config, settings=settings, selected_computers=["tigu"]
    )
    assert not ok


def test_unused_tigu_fields_do_not_block_local_only_configuration(onboarding_config):
    ok, message, _ = control.get_config(
        file_path=onboarding_config, selected_computers=["mac-localhost"]
    )
    assert ok, message


def test_widget_is_opt_in_and_waits_for_registration(tmp_path, monkeypatch):
    public = run_key_script(tmp_path / "keys").stdout.strip()
    calls = []
    monkeypatch.setattr(ssh_onboarding, "key_is_valid", lambda path: True)
    monkeypatch.setattr(
        ssh_onboarding, "prepare_ela_key", lambda *args: calls.append(args) or public
    )
    prepared = []
    widget = ssh_onboarding.TiguKeyPreparation(
        username=lambda: "cscsaccount",
        empa_username=lambda: "empaaccount",
        certificate=lambda: "certificate.pub",
        prepared=prepared.append,
        host="ela.cscs.ch",
        contact="aliaksandr.yakutovich@empa.ch",
    )
    try:
        assert not calls and not widget.request.value
        widget.button.click()
        assert calls == [("cscsaccount", "ela.cscs.ch")]
        assert prepared == [ssh_onboarding.ELA_KEY_PATH]
        assert public in widget.request.value
        assert "aliaksandr.yakutovich@empa.ch" in widget.request.value
        assert "Empa/Tigu username: empaaccount" in widget.request.value
        assert (
            "Please add the following public SSH key to my account on both the jump host and Tigu."
            in widget.request.value
        )
        assert "wait for confirmation" in widget.status.value
        monkeypatch.setattr(ssh_onboarding, "key_is_valid", lambda path: False)
        widget.button.click()
        assert len(calls) == 1 and "renew" in widget.status.value
        assert not widget.request.value
    finally:
        widget.close()
