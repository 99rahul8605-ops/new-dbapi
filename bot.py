#!/usr/bin/env python3
"""Owner-only Telegram RAR receiver with restart-safe, chunked downloads."""
import asyncio
import contextlib
import fcntl
import json
import logging
import os
import re
import shutil
import stat
import time
import uuid
from pathlib import Path, PurePosixPath

import rarfile
from dotenv import load_dotenv
from pyrogram import Client, filters, idle

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("db-upload")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
BASE = Path(os.getenv("DATA_DIR", "./database_uploads")).expanduser().resolve()
MAX_GB = float(os.getenv("MAX_EXTRACT_GB", "20"))
MAX_FILES = int(os.getenv("MAX_FILES", "100000"))
MAX_BYTES = int(MAX_GB * 1024 ** 3)
KEEP_RAR = os.getenv("KEEP_RAR", "false").lower() == "true"
RETRIES = max(1, min(10, int(os.getenv("DOWNLOAD_RETRIES", "5"))))
CHUNK = 1024 * 1024  # Pyrofork stream_media uses 1 MiB chunk offsets.
UPDATE_SECONDS = 8

if not all((BOT_TOKEN, API_ID, API_HASH, OWNER_ID)):
    raise SystemExit("Set BOT_TOKEN, API_ID, API_HASH and OWNER_ID in .env")
if MAX_BYTES <= 0 or MAX_FILES <= 0:
    raise SystemExit("MAX_EXTRACT_GB and MAX_FILES must be positive")

JOBS = BASE / ".jobs"
PARTS = BASE / ".incoming"
ARCHIVES = BASE / ".archives"
SECRETS = BASE / ".secrets"
for folder in (BASE, JOBS, PARTS, ARCHIVES, SECRETS):
    folder.mkdir(parents=True, exist_ok=True)
SECRETS.chmod(0o700)

# Fail fast if the same bot accidentally starts in two screen sessions.
lockfile = (BASE / ".instance.lock").open("a+")
try:
    fcntl.flock(lockfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit("Another uploader already uses this DATA_DIR")

client = Client("upload_bot_pyrofork_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=8)
queue = asyncio.Queue()
queued_ids = set()
active_id = None
pause_requested = set()
last_progress = {}
worker_task = None
known_jobs = {}


def safe_name(value):
    value = Path(str(value).replace("\\", "/")).name
    value = re.sub(r"[^a-zA-Z0-9._-]", "_", value)
    value = value.strip("._")[:110]
    return value or "database"


def job_path(job_id):
    if not re.fullmatch(r"[0-9a-f]{12}", job_id):
        raise ValueError("Invalid job ID")
    return JOBS / (job_id + ".json")


def save_job(job):
    dest = job_path(job["id"])
    tmp = dest.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(job, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, dest)
    known_jobs[job["id"]] = job


def load_jobs():
    for path in JOBS.glob("*.json"):
        try:
            job = json.loads(path.read_text(encoding="utf-8"))
            if job.get("id") != path.stem:
                raise ValueError("Job ID mismatch")
            known_jobs[job["id"]] = job
        except (OSError, ValueError, KeyError) as exc:
            log.error("Could not read job %s: %s", path, exc)


def part_path(job):
    return PARTS / (job["id"] + ".part")


def output_path(job):
    return BASE / job["id"]


def archive_path(job):
    return ARCHIVES / (job["id"] + "_" + safe_name(job["name"]))


def bytes_human(value):
    return f"{value / 1024**2:.1f} MiB"


def queue_job(job):
    if job["id"] == active_id or job["id"] in queued_ids:
        return False
    job["state"] = "queued"
    save_job(job)
    queued_ids.add(job["id"])
    queue.put_nowait(job["id"])
    return True


def password_file(job):
    return SECRETS / (job["id"] + ".password")


def set_password(job, password):
    file = password_file(job)
    tmp = file.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(password)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, file)
    finally:
        tmp.unlink(missing_ok=True)


def get_password(job):
    file = password_file(job)
    return file.read_text(encoding="utf-8") if file.exists() else None


def validate_entry(info):
    raw = info.filename.replace("\\", "/")
    path = PurePosixPath(raw)
    if not raw or raw.startswith("/") or re.match(r"^[A-Za-z]:", raw):
        raise ValueError("Unsafe absolute path")
    if any(part in ("", ".", "..") for part in raw.rstrip("/").split("/")):
        raise ValueError("Unsafe archive path")
    if len(path.parts) > 30 or len(raw) > 1000:
        raise ValueError("Archive path too long")
    if getattr(info, "is_symlink", lambda: False)():
        raise ValueError("Symbolic links are not allowed")
    mode = getattr(info, "mode", 0) or 0
    if stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR):
        raise ValueError("Special archive entries are not allowed")
    return path


def extract_archive(source, destination, password=None):
    """Extract into disposable staging dir, only publish completed directories."""
    stage = BASE / (".extracting_" + destination.name)
    if destination.exists():
        raise ValueError("Destination already exists")
    shutil.rmtree(stage, ignore_errors=True)
    stage.mkdir()
    try:
        with rarfile.RarFile(str(source)) as archive:
            if password is not None:
                archive.setpassword(password)
            elif archive.needs_password():
                raise ValueError("PASSWORD_REQUIRED")
            entries = archive.infolist()
            if len(entries) > MAX_FILES:
                raise ValueError("Too many archive entries")
            total = 0
            seen = set()
            for entry in entries:
                rel = validate_entry(entry)
                key = str(rel).casefold()
                if key in seen:
                    raise ValueError("Duplicate archive entries")
                seen.add(key)
                if not entry.isdir():
                    total += entry.file_size
                    if total > MAX_BYTES:
                        raise ValueError(f"Extracted size exceeds {MAX_GB:g} GiB")
            if shutil.disk_usage(BASE).free < total + 128 * 1024**2:
                raise ValueError("Not enough free disk space for extraction")
            actual = 0
            count = 0
            for entry in entries:
                rel = validate_entry(entry)
                dest = stage.joinpath(*rel.parts)
                if entry.isdir():
                    dest.mkdir(parents=True, exist_ok=True)
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                written = 0
                with archive.open(entry) as src, dest.open("xb") as dst:
                    while True:
                        data = src.read(1024 * 1024)
                        if not data:
                            break
                        written += len(data)
                        actual += len(data)
                        if written > entry.file_size or actual > MAX_BYTES:
                            raise ValueError("RAR content exceeds declared size")
                        dst.write(data)
                if written != entry.file_size:
                    raise ValueError("RAR content length mismatch")
                count += 1
        os.replace(stage, destination)
        return count, actual
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


async def send_notice(text, job=None):
    try:
        if job and job.get("progress_msg"):
            await client.edit_message_text(OWNER_ID, job["progress_msg"], text)
        else:
            msg = await client.send_message(OWNER_ID, text)
            if job:
                job["progress_msg"] = msg.id
                save_job(job)
    except Exception:
        # If the status message was deleted, create another.
        try:
            msg = await client.send_message(OWNER_ID, text)
            if job:
                job["progress_msg"] = msg.id
                save_job(job)
        except Exception:
            log.exception("Could not update Telegram progress")


async def transfer(job):
    path = part_path(job)
    expected = int(job["size"])
    if expected <= 0:
        raise ValueError("Invalid Telegram file size")
    # Need enough space for the remaining archive before starting.
    existing = path.stat().st_size if path.exists() else 0
    if existing > expected:
        raise ValueError("Partial file larger than Telegram original; delete it manually")
    if shutil.disk_usage(BASE).free < expected - existing + 64 * 1024**2:
        raise ValueError("Insufficient free disk space for RAR download")

    for attempt in range(1, RETRIES + 1):
        if job["id"] in pause_requested:
            raise InterruptedError("Paused by owner")
        try:
            # Refresh the Telegram file reference after reconnect or restart.
            msg = await client.get_messages(OWNER_ID, int(job["message_id"]))
            if not msg or getattr(msg, "empty", False) or not msg.document or msg.document.file_size != expected:
                raise RuntimeError("Original Telegram message missing/changed")
            offset = path.stat().st_size if path.exists() else 0
            if offset > expected:
                raise ValueError("Invalid partial-file size")
            if offset == expected:
                return path
            aligned = offset - offset % CHUNK
            if aligned != offset:
                with path.open("r+b") as fh:
                    fh.truncate(aligned)
                offset = aligned
            started = time.monotonic()
            last_emit = 0.0
            last_sample = started
            last_bytes = offset
            with path.open("ab") as fh:
                async for chunk in client.stream_media(msg, offset=offset // CHUNK):
                    if job["id"] in pause_requested:
                        raise InterruptedError("Paused by owner")
                    if not chunk:
                        continue
                    if offset + len(chunk) > expected:
                        raise RuntimeError("Received more bytes than expected")
                    fh.write(chunk)
                    offset += len(chunk)
                    now = time.monotonic()
                    if now - last_emit >= UPDATE_SECONDS or offset == expected:
                        fh.flush()
                        speed = (offset - last_bytes) / max(now - last_sample, 0.01)
                        average = (offset - aligned) / max(now - started, 0.01)
                        rate = speed if speed > 0 else average
                        remaining = max(expected - offset, 0)
                        eta = int(remaining / rate) if rate > 0 else None
                        progress = (
                            f"⬇️ Downloading {job['name']}\n"
                            f"ID: {job['id']}\n"
                            f"{offset * 100 / expected:.1f}% · {bytes_human(offset)} / {bytes_human(expected)}\n"
                            f"Speed: {rate / 1024**2:.2f} MiB/s · "
                            f"ETA: {eta // 60}m {eta % 60}s" if eta is not None else
                            f"⬇️ Downloading {job['name']} · ETA: calculating"
                        )
                        # Notice failures must not break a file transfer.
                        await send_notice(progress, job)
                        last_emit, last_sample, last_bytes = now, now, offset
                fh.flush()
                os.fsync(fh.fileno())
            if path.stat().st_size != expected:
                raise RuntimeError("Incomplete Telegram transfer")
            return path
        except InterruptedError:
            raise
        except Exception as exc:
            log.warning("Download %s attempt %d/%d: %s", job["id"], attempt, RETRIES, exc)
            if attempt == RETRIES:
                raise
            await send_notice(
                f"🔄 Network interruption for {job['id']}; retry {attempt}/{RETRIES}. "
                "Saved bytes will be resumed.", job
            )
            await asyncio.sleep(min(60, 2 ** attempt))
    raise RuntimeError("Retry budget exhausted")


async def process_job(job):
    job["state"] = "downloading"
    job.pop("error", None)
    save_job(job)
    await send_notice(f"⏳ Starting/resuming {job['name']}\nID: {job['id']}", job)
    try:
        archive = await transfer(job)
        job["state"] = "extracting"
        save_job(job)
        await send_notice(f"📂 Download finished. Extracting {job['name']}...", job)
        count, size = await asyncio.to_thread(extract_archive, archive, output_path(job), get_password(job))
        job["state"] = "done"
        password_file(job).unlink(missing_ok=True)
        job["files"] = count
        job["extracted_bytes"] = size
        job["completed_at"] = int(time.time())
        save_job(job)
        if KEEP_RAR:
            shutil.move(str(archive), str(archive_path(job)))
        else:
            archive.unlink(missing_ok=True)
        await send_notice(
            f"✅ Completed: {job['name']}\nID: {job['id']}\n"
            f"Files: {count} · Unpacked: {bytes_human(size)}\n"
            f"Server folder: {output_path(job)}", job
        )
    except InterruptedError:
        job["state"] = "paused"
        save_job(job)
        await send_notice(f"⏸ Paused {job['id']}. /resume to continue.", job)
    except Exception as exc:
        log.error("Job %s failed (%s)", job["id"], type(exc).__name__)
        job["state"] = "failed"
        error_text = str(exc)
        if "PASSWORD_REQUIRED" in error_text or isinstance(exc, rarfile.PasswordRequired):
            job["state"] = "awaiting_password"
            job["error"] = "Password required"
            guidance = f"🔐 Archive needs a password. Use /password {job['id']} YOUR_PASSWORD."
        elif get_password(job) is not None and isinstance(exc, (rarfile.BadRarFile, rarfile.RarExecError)):
            job["state"] = "awaiting_password"
            job["error"] = "Decryption failed; check archive password"
            guidance = f"🔐 Check the password. Use /password {job['id']} CORRECT_PASSWORD."
        else:
            job["error"] = error_text[:350]
            guidance = f"❌ Failed {job['id']}: {job['error']}\nUse /retry {job['id']}."
        save_job(job)
        await send_notice(guidance + "\nDownloaded RAR kept on server.", job)
    finally:
        pause_requested.discard(job["id"])


async def worker():
    global active_id
    while True:
        job_id = await queue.get()
        queued_ids.discard(job_id)
        try:
            job = known_jobs.get(job_id)
            if job and job["state"] == "queued":
                active_id = job_id
                await process_job(job)
        except Exception:
            log.exception("Unexpected worker error for %s", job_id)
        finally:
            active_id = None
            queue.task_done()


async def authorized(message):
    if message.chat and message.chat.type.value == "private" and message.from_user and message.from_user.id == OWNER_ID:
        return True
    if message.chat and message.chat.type.value == "private":
        await message.reply_text("⛔ Access denied.")
    return False


def job_status(job):
    partial = part_path(job)
    current = partial.stat().st_size if partial.exists() else 0
    expected = int(job["size"])
    size_info = f"{bytes_human(current)} / {bytes_human(expected)}"
    if job["state"] == "done":
        size_info = f"{job.get('files', 0)} extracted files"
    info = f"{job['id']} · {job['state']} · {job['name']}\n{size_info}"
    if job.get("error"):
        info += "\nError: " + job["error"][:100]
    return info


HELP = (
    "📦 Smart Database RAR Bot\n\n"
    "Send a single .rar document in this private chat.\n"
    "Downloads resume after restart; RAR is safely extracted when finished.\n\n"
    "/status - current job, disk, queue\n"
    "/list - recent uploads\n"
    "/pause - pause current download\n"
    "/resume - continue paused/failed downloads\n"
    "/retry JOB_ID - requeue a specific failed job\n"
    "/password JOB_ID PASSWORD - unlock encrypted RAR\n"
    "/help - this help\n\n"
    "Archives are only extracted; no database engine is modified."
)


@client.on_message(filters.private & filters.command(["start", "help"]))
async def on_help(_, message):
    if await authorized(message):
        await message.reply_text(HELP)


@client.on_message(filters.private & filters.command("status"))
async def on_status(_, message):
    if not await authorized(message):
        return
    info = (
        f"📊 Bot: online\nActive: {active_id or 'none'}\n"
        f"Queue: {queue.qsize()}\nFree: {shutil.disk_usage(BASE).free / 1024**3:.2f} GiB\n"
        f"Storage: {BASE}"
    )
    if active_id and active_id in known_jobs:
        info += "\n\n" + job_status(known_jobs[active_id])
    await message.reply_text(info)


@client.on_message(filters.private & filters.command("list"))
async def on_list(_, message):
    if not await authorized(message):
        return
    jobs = sorted(known_jobs.values(), key=lambda j: j["created"], reverse=True)[:10]
    await message.reply_text("\n\n".join(job_status(j) for j in jobs) if jobs else "No uploads yet.")


@client.on_message(filters.private & filters.command("pause"))
async def on_pause(_, message):
    if not await authorized(message):
        return
    if not active_id or known_jobs[active_id]["state"] != "downloading":
        await message.reply_text("No active download to pause.")
        return
    pause_requested.add(active_id)
    await message.reply_text(f"⏸ Pausing {active_id} at next chunk boundary...")


@client.on_message(filters.private & filters.command("resume"))
async def on_resume(_, message):
    if not await authorized(message):
        return
    count = 0
    for job in sorted(known_jobs.values(), key=lambda j: j["created"]):
        if job["state"] in ("paused", "failed"):
            count += bool(queue_job(job))
    await message.reply_text(f"Queued {count} paused/failed upload(s).")


@client.on_message(filters.private & filters.command("retry"))
async def on_retry(_, message):
    if not await authorized(message):
        return
    job_id = message.command[1].lower() if len(message.command) > 1 else ""
    job = known_jobs.get(job_id)
    if not job:
        await message.reply_text("Usage: /retry JOB_ID (see /list)")
        return
    if job["state"] not in ("paused", "failed"):
        await message.reply_text(f"Cannot retry a job in {job['state']} state")
        return
    await message.reply_text("✅ Queued for retry." if queue_job(job) else "Already queued.")


@client.on_message(filters.private & filters.command("password"))
async def on_password(_, message):
    if not await authorized(message):
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.reply_text("Usage: /password JOB_ID PASSWORD. Use /list for job IDs.")
        return
    job = known_jobs.get(parts[1].lower())
    if not job or job["state"] not in ("awaiting_password", "failed", "paused"):
        await message.reply_text("Job not found or cannot accept a password. Use /list.")
        return
    if not parts[2] or len(parts[2]) > 512:
        await message.reply_text("Password must be 1-512 characters.")
        return
    set_password(job, parts[2])
    deleted = False
    try:
        await message.delete(revoke=True)
        deleted = True
    except Exception:
        pass
    await client.send_message(
        OWNER_ID,
        ("🔐 Password saved and extraction queued. " if deleted else
         "🔐 Password saved. Please delete your password message manually. ")
        + "Job: " + job["id"]
    )
    queue_job(job)


@client.on_message(filters.private & filters.document)
async def on_file(_, message):
    if not await authorized(message):
        return
    document = message.document
    filename = document.file_name or ""
    if not filename.lower().endswith(".rar"):
        await message.reply_text("Send a single .rar archive as a Telegram document.")
        return
    if not document.file_size:
        await message.reply_text("Telegram didn't report the document size.")
        return
    if any(j["message_id"] == message.id for j in known_jobs.values()):
        await message.reply_text("Already accepted this Telegram message.")
        return
    job = {
        "id": uuid.uuid4().hex[:12],
        "name": safe_name(filename) if safe_name(filename).lower().endswith(".rar") else safe_name(filename) + ".rar",
        "message_id": message.id,
        "size": document.file_size,
        "state": "queued",
        "created": int(time.time()),
    }
    save_job(job)
    queue_job(job)
    await message.reply_text(
        f"✅ File queued: {job['name']}\nID: {job['id']}\n"
        f"Size: {bytes_human(job['size'])}\n"
        "Safe resumable download enabled."
    )


async def main():
    global worker_task
    load_jobs()
    await client.start()
    me = await client.get_me()
    log.info("Started @%s; %s storage; %d saved jobs", me.username, BASE, len(known_jobs))
    worker_task = asyncio.create_task(worker())
    # Recover interrupted transfers. Extraction can be rerun from completed .part.
    for job in sorted(known_jobs.values(), key=lambda j: j["created"]):
        if job["state"] in ("queued", "downloading", "extracting"):
            queue_job(job)
    try:
        await idle()
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
