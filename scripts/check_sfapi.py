#!/usr/bin/env python3
"""Check NERSC SFAPI credentials using the official sfapi_client."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


DEFAULT_KEY_PATH = Path(__file__).resolve().parents[1] / "sf_api.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exchange an SFAPI client_id/JWK credential file for a bearer token "
            "and call harmless account endpoints."
        )
    )
    parser.add_argument(
        "key_path",
        nargs="?",
        type=Path,
        help="Path to SFAPI JSON credentials. Defaults to repo-local sf_api.json.",
    )
    parser.add_argument(
        "--key",
        dest="key_override",
        type=Path,
        help="Path to SFAPI JSON credentials. Overrides the positional path.",
    )
    parser.add_argument(
        "--show-token",
        action="store_true",
        help="Print the full short-lived bearer token instead of a redacted prefix.",
    )
    return parser.parse_args()


def credential_path(args: argparse.Namespace) -> Path:
    path = args.key_override or args.key_path or DEFAULT_KEY_PATH
    return path.expanduser().resolve()


def validate_credential_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit(
            f"Credential file not found: {path}\n\n"
            "Create it with this shape:\n"
            '{\n  "client_id": "your_client_id",\n'
            '  "secret": {"kty": "RSA", "...": "..."}\n}\n'
        )

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise SystemExit(f"Credential file is not valid JSON: {path}: {err}") from err

    missing = [key for key in ("client_id", "secret") if key not in data]
    if missing:
        raise SystemExit(
            f"Credential file is missing required field(s): {', '.join(missing)}"
        )


def redact_token(token: str) -> str:
    if len(token) <= 16:
        return "<short token>"
    return f"{token[:12]}...{token[-8:]}"


def main() -> int:
    args = parse_args()
    key = credential_path(args)
    validate_credential_file(key)

    try:
        from sfapi_client import Client
    except ModuleNotFoundError as err:
        raise SystemExit(
            "Missing dependency: sfapi_client\n"
            "Install it with: python -m pip install sfapi-client"
        ) from err

    with Client(key=key) as client:
        token = client.token
        print(f"Credential file: {key}")
        if args.show_token:
            print(f"Bearer token: {token}")
        else:
            print(f"Bearer token: {redact_token(token)}")

        account = client.get("account").json()
        projects = client.get("account/projects").json()

    print("\nAccount:")
    print(json.dumps(account, indent=2, sort_keys=True))

    print("\nProjects:")
    print(json.dumps(projects, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
