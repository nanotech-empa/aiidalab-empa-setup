"""Explicit new-user Tigu key preparation; private material stays on ELA."""

import base64
import re
import shlex
from html import escape

import ipywidgets as ipw

from .aiida_and_ssh_utils import add_to_known_hosts, key_is_valid, run_command

ELA_KEY_PATH = "~/.ssh/aiidalab-tigu/id_ed25519"
REMOTE_KEY_SCRIPT = r"""
set -euo pipefail
key_dir="$1"
parent="$(dirname "$key_dir")"
if [ -L "$parent" ] || [ -L "$key_dir" ]; then
    echo "Refusing a symbolic-link key directory; review it manually." >&2
    exit 1
fi
if [ -e "$key_dir" ] && { [ ! -d "$key_dir" ] || [ ! -O "$key_dir" ]; }; then
    echo "Key directory is not a directory owned by your account." >&2
    exit 1
fi
umask 077
mkdir -p "$key_dir"
chmod 700 "$key_dir"
key="$key_dir/id_ed25519"
pub="$key.pub"
if [ -L "$key" ] || [ -L "$pub" ]; then
    echo "Refusing symbolic-link key files." >&2
    exit 1
fi
if [ -e "$key" ] || [ -e "$pub" ]; then
    if [ ! -f "$key" ] || [ ! -f "$pub" ] || [ ! -O "$key" ] || [ ! -O "$pub" ]; then
        echo "An incomplete or unexpected key pair exists; nothing was overwritten." >&2
        exit 1
    fi
else
    ssh-keygen -q -t ed25519 -N '' -C 'AiiDAlab Tigu access' -f "$key"
fi
chmod 600 "$key"
chmod 644 "$pub"
private_fp="$(ssh-keygen -y -P '' -f "$key" | ssh-keygen -lf - | awk '{print $2}')"
public_fp="$(ssh-keygen -lf "$pub" | awk '{print $2}')"
if [ "$private_fp" != "$public_fp" ]; then
    echo "The existing public/private key fingerprints differ; review them manually." >&2
    exit 1
fi
cat "$pub"
"""


def prepare_ela_key(username, host="ela.cscs.ch"):
    """Create/reuse only the dedicated app key, returning its public part."""
    if (
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9._-]*", username or "")
        or username == "select"
    ):
        raise ValueError("Choose your CSCS username before preparing a key.")
    if host != "ela.cscs.ch":
        raise ValueError("Tigu key preparation must use the configured CSCS ELA host.")
    if not add_to_known_hosts(["ssh-keyscan", "-H", host]):
        raise RuntimeError(
            "ELA host-key verification failed. Review the SSH connection help."
        )
    remote = (
        "/bin/bash -c "
        + shlex.quote(REMOTE_KEY_SCRIPT)
        + ' -- "$HOME/.ssh/aiidalab-tigu"'
    )
    output, ok = run_command(
        [
            "ssh",
            "-oForwardAgent=no",
            "-l",
            username,
            "-i",
            "/home/jovyan/.ssh/cscs-key",
            host,
            remote,
        ]
    )
    if not ok:
        raise RuntimeError(f"Key preparation on ELA failed: {output}")
    matches = [
        line.strip() for line in output.splitlines() if line.startswith("ssh-ed25519 ")
    ]
    if len(matches) != 1:
        raise RuntimeError("ELA did not return exactly one Ed25519 public key.")
    public_key = matches[0]
    try:
        blob = base64.b64decode(public_key.split()[1], validate=True)
    except (ValueError, IndexError) as error:
        raise RuntimeError("ELA returned an invalid public key.") from error
    if (
        not blob.startswith(b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20")
        or len(blob) != 51
    ):
        raise RuntimeError("ELA returned an invalid Ed25519 public-key structure.")
    return public_key


def activation_request(public_key, cscs_user, empa_user, recipient):
    return (
        f"To: {recipient}\n"
        "Subject: Activate SSH key on the jump host for Tigu access\n\n"
        "Hello Aliaksandr,\n\n"
        "Please activate the following public SSH key on the jump host for my Tigu access.\n"
        f"CSCS username: {cscs_user}\nEmpa/Tigu username: {empa_user}\n\n"
        f"{public_key}\n\n"
        "Please also confirm whether this key is authorized on Tigu, or whether its "
        "administrator needs to add it there separately. Let me know when access is active.\n\n"
        "Thank you."
    )


class TiguKeyPreparation(ipw.VBox):
    """Opt-in key creation and a public request to copy; never sends email."""

    def __init__(self, username, empa_username, certificate, prepared, host, contact):
        self.username = username
        self.empa_username = empa_username
        self.certificate = certificate
        self.prepared = prepared
        self.host = host
        self.contact = contact
        self.status = ipw.HTML()
        self.request = ipw.Textarea(
            value="", disabled=True, layout=ipw.Layout(width="100%", height="240px")
        )
        self.request.layout.display = "none"
        self.button = ipw.Button(
            description="Prepare new Tigu key on ELA", layout=ipw.Layout(width="280px")
        )
        self.button.on_click(self.prepare)
        explanation = ipw.HTML(
            "<b>New Tigu access</b><br>Existing ELA-key access can keep its current route. "
            "For new access, this action creates or reuses a dedicated Ed25519 key on ELA, "
            "without a passphrase so the proxy can load it non-interactively. "
            "Its private file stays on ELA; existing default keys are preserved. "
            f"Copy the public-key request below and send it yourself to <b>{escape(contact)}</b>."
        )
        super().__init__([explanation, self.button, self.status, self.request])

    def prepare(self, _):
        self.button.disabled = True
        self.request.value = ""
        self.request.layout.display = "none"
        try:
            if not key_is_valid(self.certificate()):
                raise ValueError(
                    "Install/open MFA CSCS and renew your CSCS certificate first."
                )
            username = self.username()
            empa_username = self.empa_username()
            if (
                not re.fullmatch(r"[A-Za-z_][A-Za-z0-9._-]*", empa_username or "")
                or empa_username == "select"
            ):
                raise ValueError(
                    "Enter your Empa/Tigu username before preparing a key."
                )
            public_key = prepare_ela_key(username, self.host)
            self.request.value = activation_request(
                public_key, username, empa_username, self.contact
            )
            self.request.layout.display = "block"
            self.prepared(ELA_KEY_PATH)
            self.status.value = (
                "Public key prepared on ELA. Send the request and wait for confirmation. "
                "Then inspect/apply the selected route to check access through the jump host to Tigu. "
                "Adding the key on Tigu requires an existing working login or its administrator; "
                "the app cannot grant first access itself."
            )
        except (OSError, ValueError, RuntimeError) as error:
            self.status.value = escape(str(error))
        finally:
            self.button.disabled = False
