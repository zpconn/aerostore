"""Install a hash-pinned TLC JAR, optionally from a hash-pinned ZIP/VSIX."""
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import urllib.request
import zipfile

MAX_BYTES = 64 * 1024 * 1024


def checked_hash(value: object, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"invalid TLC {field}: expected a SHA-256 digest")
    return value


def validate_pin(pin: dict) -> None:
    checked_hash(pin.get("sha256"), "sha256")
    if not isinstance(pin.get("release_url"), str) or not pin["release_url"].startswith("https://"):
        raise ValueError("TLC release_url must use HTTPS")
    member = pin.get("archive_member")
    if "archive_member" in pin or "archive_sha256" in pin:
        if (not isinstance(member, str) or not member or "\\" in member
                or PurePosixPath(member).is_absolute() or ".." in PurePosixPath(member).parts
                or PurePosixPath(member).as_posix() != member or member == "."):
            raise ValueError("invalid TLC archive_member: expected one relative file path")
        checked_hash(pin.get("archive_sha256"), "archive_sha256")


def bounded_read(stream, description: str) -> bytes:
    value = stream.read(MAX_BYTES + 1)
    if len(value) > MAX_BYTES:
        raise ValueError(f"{description} exceeds the {MAX_BYTES}-byte limit")
    return value


def verify_hash(value: bytes, expected: str, description: str) -> None:
    if hashlib.sha256(value).hexdigest() != expected:
        raise ValueError(f"{description} SHA-256 mismatch")


def ensure_tlc(pin: dict, jar: Path, *, allow_download: bool = False) -> None:
    """Verify an existing JAR or atomically create a missing one; never replace."""
    validate_pin(pin)
    if jar.is_symlink():
        raise ValueError(f"refusing a symlink TLC destination: {jar}")
    if jar.exists():
        with jar.open("rb") as installed:
            verify_hash(bounded_read(installed, "installed TLC JAR"), pin["sha256"], "installed TLC JAR")
        return
    if not allow_download:
        raise ValueError(f"missing TLC jar: {jar}; rerun with --download")
    with urllib.request.urlopen(pin["release_url"], timeout=60) as response:
        payload = bounded_read(response, "TLC download")
    if "archive_member" in pin:
        verify_hash(payload, pin["archive_sha256"], "TLC archive")
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                members = [entry for entry in archive.infolist() if entry.filename == pin["archive_member"]]
                if len(members) != 1 or members[0].is_dir():
                    raise ValueError("TLC archive must contain exactly one pinned JAR member")
                if members[0].file_size > MAX_BYTES:
                    raise ValueError("TLC archive member exceeds the byte limit")
                with archive.open(members[0]) as member:
                    payload = bounded_read(member, "TLC archive member")
        except (zipfile.BadZipFile, RuntimeError, NotImplementedError) as error:
            raise ValueError(f"cannot read pinned TLC archive member: {error}") from error
    verify_hash(payload, pin["sha256"], "downloaded TLC JAR")
    jar.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix=f".{jar.name}.", dir=jar.parent, delete=False) as output:
            temporary = Path(output.name)
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        # A same-filesystem hard link publishes the complete file atomically and
        # fails if another process created the destination. replace() would
        # silently overwrite an installed tool or retained evidence in that race.
        os.link(temporary, jar)
    finally:
        if temporary is not None:
            temporary.unlink()
