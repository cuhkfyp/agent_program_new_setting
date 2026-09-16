"""Interactive, secret-safe CCD agent configuration."""

from __future__ import annotations

import argparse
import getpass
from urllib.parse import urlparse

import keyring


SERVICE = "ccd_agent"


def _prompt(label: str, supplied: str = "", default: str = "") -> str:
    if supplied:
        return supplied.strip()
    suffix = f" [{default}]" if default else ""
    return (input(f"{label}{suffix}: ").strip() or default).strip()


def _validate_url(value: str) -> str:
    value = value.strip().rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("ERPNext URL must include http:// or https:// and a host")
    return value


def configure(
    *, url: str = "", user: str = "", site: str = "", namespace: str = ""
) -> None:
    endpoint = _validate_url(_prompt("ERPNext URL", url))
    username = _prompt("ERPNext integration username", user)
    if not username:
        raise ValueError("ERPNext integration username is required")
    password = getpass.getpass("ERPNext integration password: ")
    if not password:
        raise ValueError("ERPNext integration password is required")
    site_name = _prompt("Frappe site name", site, "frontend")
    socket_namespace = _prompt(
        "Socket.IO namespace", namespace, f"/{site_name}"
    )
    if not socket_namespace.startswith("/"):
        socket_namespace = "/" + socket_namespace

    keyring.set_password(SERVICE, "erpnext_url", endpoint)
    keyring.set_password(SERVICE, "erpnext_user", username)
    keyring.set_password(SERVICE, "erpnext_pass", password)
    keyring.set_password(SERVICE, "site_name", site_name)
    keyring.set_password(SERVICE, "socket_namespace", socket_namespace)
    print("CCD agent settings were stored in the operating-system credential store.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="")
    parser.add_argument("--user", default="")
    parser.add_argument("--site", default="")
    parser.add_argument("--namespace", default="")
    args = parser.parse_args()
    configure(
        url=args.url,
        user=args.user,
        site=args.site,
        namespace=args.namespace,
    )


if __name__ == "__main__":
    main()
