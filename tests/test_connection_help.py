from empa_setup_utils import aiida_and_ssh_utils
from empa_setup_utils.connection_help import connection_guide, connection_help_html

GUIDES = {
    "alps": {
        "title": "ALPS",
        "steps": ["Install MFA CSCS and refresh the CSCS certificate."],
    },
    "tigu": {
        "title": "Tigu",
        "steps": [
            "Use the CSCS key and a separate Empa SSH key.",
            "Send only the public .pub key to SCI-IT for the jump host.",
        ],
    },
}


def test_failed_tigu_connection_reports_its_separate_key_requirement(
    monkeypatch, capsys
):
    computers = {
        "tigu": {
            "ssh_help": "tigu",
            "ssh_host": "tigu",
            "setup": {"hostname": "tigu.example", "transport": "core.ssh"},
            "config": {"proxy_command": "existing-user-proxy"},
        }
    }
    monkeypatch.setattr(
        aiida_and_ssh_utils,
        "run_command",
        lambda args: ("Permission denied (publickey)", False),
    )
    assert not aiida_and_ssh_utils.set_ssh(computers, ["tigu"], GUIDES)
    output = capsys.readouterr().out
    assert "separate Empa SSH key" in output and "public .pub key to SCI-IT" in output
    assert "SSH help for tigu" in output


def test_failed_alps_bootstrap_reports_mfa_instructions(monkeypatch, capsys):
    computers = {
        "daint": {
            "ssh_help": "alps",
            "setup": {"hostname": "daint.alps.cscs.ch"},
            "config": {"proxy_jump": "ela.cscs.ch"},
        }
    }
    monkeypatch.setattr(
        aiida_and_ssh_utils, "add_to_known_hosts", lambda *args, **kwargs: False
    )
    assert not aiida_and_ssh_utils.set_ssh(computers, ["daint"], GUIDES)
    output = capsys.readouterr().out
    assert "Install MFA CSCS" in output and "refresh" in output
    assert "SCI-IT" not in output


def test_other_computers_get_generic_help_without_cscs_assumptions():
    guide = connection_guide({"setup": {"hostname": "elsewhere"}}, GUIDES)
    assert "administrator" in " ".join(guide["steps"])
    assert "CSCS" not in " ".join(guide["steps"])


def test_help_renders_configuration_as_text_and_filters_link_schemes():
    computer = {
        "ssh_help": {
            "title": "Example",
            "steps": ["Keep <private> & public distinct"],
            "links": [
                {"label": "bad", "url": "javascript:alert(1)"},
                {"label": "MFA & help", "url": "https://example.org/help"},
            ],
        }
    }
    html = connection_help_html(computer)
    assert "&lt;private&gt; &amp; public" in html and "<private>" not in html
    assert "javascript:" not in html
    assert "MFA &amp; help" in html and 'href="https://example.org/help"' in html
