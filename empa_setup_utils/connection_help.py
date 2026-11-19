"""Render computer-specific connection prerequisites from the configuration."""

from html import escape
from urllib.parse import urlsplit


def connection_guide(computer, guides=None):
    """Return a configured guide, with generic help for other remote computers."""
    reference = computer.get("ssh_help")
    guide = reference if isinstance(reference, dict) else (guides or {}).get(reference)
    return guide or {
        "title": "SSH connection help",
        "steps": [
            "Use the SSH credentials and connection route provided by this computer's administrator.",
            "If access fails, contact that administrator with the non-secret SSH error message.",
        ],
    }


def connection_help_html(computer, guides=None):
    guide = connection_guide(computer, guides)
    steps = "".join(f"<li>{escape(step)}</li>" for step in guide.get("steps", []))
    links = " ".join(
        f'<a href="{escape(link["url"], quote=True)}" target="_blank" rel="noopener noreferrer">{escape(link["label"])}</a>'
        for link in guide.get("links", [])
        if urlsplit(link["url"]).scheme in {"http", "https"}
    )
    return f"<ol>{steps}</ol>{links}"


def print_connection_help(name, computer, guides=None):
    guide = connection_guide(computer, guides)
    print(f"SSH help for {name}: {guide['title']}")
    for step in guide.get("steps", []):
        print(f"• {step}")
    for link in guide.get("links", []):
        if urlsplit(link["url"]).scheme in {"http", "https"}:
            print(f"{link['label']}: {link['url']}")
