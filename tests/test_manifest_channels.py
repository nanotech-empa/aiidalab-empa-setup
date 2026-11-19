"""Exercise real stdout/stderr separation without a remote job or profile."""

import json
import subprocess
import sys

import pytest

from empa_setup_utils import aiida_and_ssh_utils as commands
from empa_setup_utils import readiness

IDENTITY = "d5ab99071241dd1479646a73b1d7a5842d96d807f1193800e01b64ef948a4920"
DIAGNOSTIC = "Identity added: proxy-key\nLifetime set to 5 seconds\n"


def probe(monkeypatch, payload, returncode=0):
    def remote(host, arguments, *, capture=False):
        assert host == "tigu"
        if not capture:
            return "", True
        assert arguments[-1].endswith("/selected/quantum-energy manifest")
        return commands.run_command_capture(
            [
                sys.executable,
                "-c",
                "import sys; sys.stdout.write(sys.argv[1]); "
                "sys.stderr.write(sys.argv[2]); sys.exit(int(sys.argv[3]))",
                payload,
                DIAGNOSTIC,
                str(returncode),
            ]
        )

    monkeypatch.setattr(readiness, "remote_command", remote)
    config = {}
    request = {
        "host": "tigu",
        "transport": "core.ssh",
        "image": None,
        "view": None,
        "executable": "/selected/quantum-energy",
        "code_key": "quantum",
        "code_config": config,
        "manifest": {"source_sha256": IDENTITY},
    }
    return readiness.manage_uenv_images([request]), config


def test_valid_manifest_uses_only_stdout_and_preserves_proxy_diagnostics(
    monkeypatch, capsys
):
    manifest = {"protocol": "quantum-energy/v1", "source_sha256": IDENTITY}
    ok, config = probe(monkeypatch, json.dumps(manifest))
    assert ok and config["_runtime_manifest"] == manifest
    assert DIAGNOSTIC.strip() in capsys.readouterr().out


@pytest.mark.parametrize(
    "payload",
    [
        "not JSON",
        '{"protocol":',
        "[]",
        "null",
        json.dumps({"protocol": "quantum-energy/v1", "source_sha256": IDENTITY})
        + "garbage",
        json.dumps({"protocol": "retired-worker/v1", "source_sha256": IDENTITY}),
        json.dumps({"protocol": "quantum-energy/v1", "source_sha256": "0" * 64}),
    ],
)
def test_malformed_or_other_identity_never_qualifies(monkeypatch, payload):
    ok, config = probe(monkeypatch, payload)
    assert not ok and "_runtime_manifest" not in config


def test_nonzero_exit_is_not_hidden_by_valid_json(monkeypatch):
    ok, config = probe(
        monkeypatch,
        json.dumps(
            {
                "protocol": "quantum-energy/v1",
                "source_sha256": IDENTITY,
            }
        ),
        returncode=1,
    )
    assert not ok and "_runtime_manifest" not in config


def test_legacy_diagnostics_and_strict_ssh_are_preserved(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        assert kwargs["stdin"] == subprocess.DEVNULL
        return subprocess.CompletedProcess(command, 0, '{"ok":true}', DIAGNOSTIC)

    monkeypatch.setattr(commands.subprocess, "run", run)
    output, ok = commands.run_command(["ssh", "tigu", "true"])
    assert ok and DIAGNOSTIC.strip() in output and '{"ok":true}' in output
    result = commands.run_command_capture(["ssh", "tigu", "true"])
    assert result.stdout == '{"ok":true}' and result.stderr == DIAGNOSTIC
    assert all(
        "-oStrictHostKeyChecking=yes" in call and "-oBatchMode=yes" in call
        for call in calls
    )
