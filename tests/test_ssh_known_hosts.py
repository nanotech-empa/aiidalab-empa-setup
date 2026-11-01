from unittest.mock import patch

import pytest

from empa_setup_utils.aiida_and_ssh_utils import add_to_known_hosts, set_ssh


def test_proxied_key_is_stored_under_the_configured_hostname(tmp_path):
    scanned_key = "10.128.1.11 ssh-ed25519 AAAATESTKEY"
    with (
        patch(
            "empa_setup_utils.aiida_and_ssh_utils.os.path.expanduser",
            side_effect=lambda path: str(tmp_path / path.removeprefix("~/")),
        ),
        patch(
            "empa_setup_utils.aiida_and_ssh_utils.run_command",
            return_value=(scanned_key, True),
        ),
    ):
        assert add_to_known_hosts(
            ["ssh", "ela.cscs.ch", "ssh-keyscan", "daint.alps.cscs.ch"],
            hostname="daint.alps.cscs.ch",
        )
    known_hosts = (tmp_path / ".ssh" / "known_hosts").read_text()
    assert known_hosts == "daint.alps.cscs.ch ssh-ed25519 AAAATESTKEY\n"


def test_set_ssh_scans_without_hashing_an_internal_address():
    config = {
        "daint": {
            "setup": {"hostname": "daint.alps.cscs.ch"},
            "config": {"proxy_jump": "ela.cscs.ch"},
        }
    }
    with (
        patch(
            "empa_setup_utils.aiida_and_ssh_utils.add_to_known_hosts", return_value=True
        ) as add,
        patch(
            "empa_setup_utils.aiida_and_ssh_utils.run_command", return_value=("", True)
        ),
    ):
        assert set_ssh(config, ["daint"])
    assert add.call_args_list[1].args == (
        ["ssh", "ela.cscs.ch", "ssh-keyscan", "daint.alps.cscs.ch"],
    )
    assert add.call_args_list[1].kwargs == {"hostname": "daint.alps.cscs.ch"}


@pytest.mark.parametrize(
    "result", [("", False), ("", True), ("# no key returned", True)]
)
def test_failed_or_empty_scans_do_not_add_a_key(tmp_path, result):
    with (
        patch(
            "empa_setup_utils.aiida_and_ssh_utils.os.path.expanduser",
            side_effect=lambda path: str(tmp_path / path.removeprefix("~/")),
        ),
        patch("empa_setup_utils.aiida_and_ssh_utils.run_command", return_value=result),
    ):
        assert not add_to_known_hosts(["ssh-keyscan", "target"], hostname="target")
    assert (tmp_path / ".ssh" / "known_hosts").read_text() == ""


def test_failed_proxy_key_scan_stops_before_the_remote_ssh_check():
    config = {
        "daint": {
            "setup": {"hostname": "daint.alps.cscs.ch"},
            "config": {"proxy_jump": "ela.cscs.ch"},
        }
    }
    with (
        patch(
            "empa_setup_utils.aiida_and_ssh_utils.add_to_known_hosts",
            return_value=False,
        ),
        patch("empa_setup_utils.aiida_and_ssh_utils.run_command") as command,
    ):
        assert not set_ssh(config, ["daint"])
        command.assert_not_called()
