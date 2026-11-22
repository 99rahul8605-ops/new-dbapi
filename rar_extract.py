"""RAR extraction using the installed unar/lsar backend.

Preflights entry names and size, extracts to a private staging directory and
publishes only a successfully verified extraction. Never executes archive files.
"""
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path, PurePosixPath


class ArchivePasswordError(ValueError):
    pass


class ArchiveExtractionError(ValueError):
    pass


def _safe_name(name):
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ArchiveExtractionError("Invalid archive path")
    name = name.replace("\\", "/")
    path = PurePosixPath(name)
    if (name.startswith("/") or re.match(r"^[A-Za-z]:", name)
            or any(p in ("", ".", "..") for p in name.rstrip("/").split("/"))
            or len(name) > 1000 or len(path.parts) > 30):
        raise ArchiveExtractionError("Unsafe archive path detected")
    return path


def _run(args, timeout):
    """Capture bounded diagnostic text without accidentally logging passwords."""
    with tempfile.TemporaryFile(mode="w+b") as err:
        try:
            result = subprocess.run(
                args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=err, timeout=timeout, check=False,
                env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8"},
            )
        except subprocess.TimeoutExpired as exc:
            raise ArchiveExtractionError("Archive tool timed out") from exc
        err.seek(0)
        error = err.read(8192).decode("utf-8", errors="replace")
    return result.returncode, result.stdout, error


def _password_error(message):
    message = message.lower()
    return any(word in message for word in ("password", "decrypt", "encrypted", "passphrase"))


def _preflight(source, password, max_files, max_bytes):
    lsar = shutil.which("lsar")
    if not lsar:
        raise ArchiveExtractionError("Missing lsar. Install: apt install unar")
    args = [lsar, "-json", "-no-recursion"]
    if password is not None:
        args += ["-password", password]
    args += [str(source)]
    code, data, errors = _run(args, 180)
    if code == 2 and _password_error(errors):
        raise ArchivePasswordError("Password required or incorrect")
    if code != 0:
        if _password_error(errors):
            raise ArchivePasswordError("Password required or incorrect")
        raise ArchiveExtractionError("Cannot list archive with lsar (invalid/corrupt archive)")
    try:
        info = json.loads(data)
        entries = info["lsarContents"]
    except (ValueError, KeyError, TypeError) as exc:
        raise ArchiveExtractionError("Unable to read lsar archive manifest") from exc
    if not isinstance(entries, list) or len(entries) > max_files:
        raise ArchiveExtractionError("Too many archive entries")
    total = 0
    filenames = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ArchiveExtractionError("Malformed archive entry")
        # lsar's JSON uses XADFileName/XADFileSize (older builds may use Name/Size).
        raw = entry.get("XADFileName", entry.get("Name"))
        if isinstance(raw, dict):
            raw = raw.get("string") or raw.get("String")
        path = _safe_name(raw)
        key = str(path).casefold()
        if key in filenames:
            raise ArchiveExtractionError("Duplicate archive file path")
        filenames.add(key)
        if entry.get("XADIsLink") or entry.get("XADIsSymbolicLink"):
            raise ArchiveExtractionError("Archive contains symbolic links")
        if entry.get("XADIsDirectory"):
            continue
        size = entry.get("XADFileSize", entry.get("Size"))
        if isinstance(size, bool) or not isinstance(size, (int, float)) or size < 0 or int(size) != size:
            raise ArchiveExtractionError("Archive file size unavailable")
        total += int(size)
        if total > max_bytes:
            raise ArchiveExtractionError("Archive exceeds configured extraction limit")
    return len(entries), total


def extract_archive(source, destination, password, max_files, max_bytes):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    unar = shutil.which("unar")
    if not unar:
        raise ArchiveExtractionError("Missing unar. Install: apt install unar")
    if not source.is_file():
        raise ArchiveExtractionError("Downloaded RAR missing")
    if destination.exists():
        raise ArchiveExtractionError("Destination already exists")
    count, planned = _preflight(source, password, max_files, max_bytes)
    if shutil.disk_usage(destination.parent).free < planned + 256 * 1024**2:
        raise ArchiveExtractionError("Not enough disk space for extracted files")
    stage = destination.parent / (".extracting_" + destination.name)
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(mode=0o700)
    try:
        args = [unar, "-quiet", "-no-directory", "-no-recursion",
                "-force-skip", "-forks", "skip", "-output-directory", str(stage)]
        if password is not None:
            # unar only exposes a password argument; this can briefly be read
            # from /proc by sufficiently privileged OS users. Restrict VPS users.
            args += ["-password", password]
        args += [str(source)]
        code, _stdout, error = _run(args, 6 * 3600)
        if code != 0:
            if _password_error(error):
                raise ArchivePasswordError("Password rejected by unar")
            raise ArchiveExtractionError("unar extraction failed (check archive or free disk)")
        files, actual = 0, 0
        for path in stage.rglob("*"):
            kind = path.lstat().st_mode
            if stat.S_ISDIR(kind):
                continue
            if not stat.S_ISREG(kind):
                raise ArchiveExtractionError("Extraction contained nonregular file/link")
            files += 1
            actual += path.stat().st_size
            if files > max_files or actual > max_bytes:
                raise ArchiveExtractionError("Extracted data exceeds configured limits")
            if not path.resolve().is_relative_to(stage.resolve()):
                raise ArchiveExtractionError("Unsafe extraction path")
        # Do not publish empty or partially extracted archives.
        if count and not files:
            raise ArchiveExtractionError("Archive contained no extracted regular files")
        os.replace(stage, destination)
        return files, actual
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
