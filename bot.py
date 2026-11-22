#!/usr/bin/env python3
"""Owner-only Telegram RAR receiver with restart-safe, chunked downloads."""
import asyncio
import contextlib
import fcntl
import json
import io
import logging
import os
import re
import shutil
import stat
import time
import uuid
from pathlib import Path, PurePosixPath

from rar_extract import extract_archive as extract_rar_unar, ArchivePasswordError, ArchiveExtractionError
from csv_audit import audit_folder, write_audit, summary as audit_summary
from sqlite_import import import_job as sqlite_import_job, status as sqlite_status, lookup as sqlite_lookup, db_path as sqlite_db_path, pending_job as sqlite_pending_job, build_username_index, username_index_status, lookup_username
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

# Pyrofork captures the event loop when the Client is created.
# Keep this loop for Client.run(main()); asyncio.run(main()) would create a
# different loop and silently break incoming update handlers.
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

client = Client("upload_bot_pyrofork_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=8)
queue = asyncio.Queue()
queued_ids = set()
active_id = None
pause_requested = set()
last_progress = {}
worker_task = None
known_jobs = {}
import_task = None
import_active = None
import_requested = set()
username_task = None


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


def extract_archive(source, destination, password=None):
    """Use the same unar backend verified manually on this server."""
    return extract_rar_unar(source, destination, password, MAX_FILES, MAX_BYTES)


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
        try:
            report = await asyncio.to_thread(audit_folder, output_path(job))
            await asyncio.to_thread(write_audit, output_path(job), report)
            job["csv_audit"] = {
                "csv_count": report["csv_count"],
                "numeric_id_samples": report["numeric_id_samples"],
                "total_samples": report["total_samples"],
            }
            save_job(job)
            await client.send_message(OWNER_ID, audit_summary(report))
        except Exception:
            log.exception("CSV audit failed for %s (extraction is still complete)", job["id"])
            await client.send_message(OWNER_ID, "⚠️ CSV audit failed; extracted files are safe. Use /audit " + job["id"])

    except InterruptedError:
        job["state"] = "paused"
        save_job(job)
        await send_notice(f"⏸ Paused {job['id']}. /resume to continue.", job)
    except Exception as exc:
        log.error("Job %s failed (%s)", job["id"], type(exc).__name__)
        job["state"] = "failed"
        error_text = str(exc)
        if isinstance(exc, ArchivePasswordError):
            job["state"] = "awaiting_password"
            job["error"] = "Password required or incorrect"
            guidance = f"🔐 Archive password needed. Use /password {job['id']} CORRECT_PASSWORD."
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
    chat = getattr(message, "chat", None)
    sender = getattr(message, "from_user", None)
    chat_type = str(getattr(chat, "type", "")).lower()
    is_private = "private" in chat_type
    if is_private and sender and int(sender.id) == OWNER_ID:
        return True
    if is_private:
        await message.reply_text("⛔ Access denied. Check OWNER_ID in .env.")
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
    "/ping - test bot response\n"
    "/audit JOB_ID - recheck extracted CSV schema\n"
    "/import JOB_ID - import extracted CSVs into private SQLite\n"
    "/importstatus - live CSV/index progress, CPU, RAM and disk I/O\n"
    "/lookup USER_ID [PAGE] - view full saved fields (owner only)\n"
    "/usernameindex - build/resume username index after import\n"
    "/user USERNAME [PAGE] - private username lookup\n"
    "/status - current job, disk, queue\n"
    "/list - recent uploads\n"
    "/pause - pause current download\n"
    "/resume - continue paused/failed downloads\n"
    "/retry JOB_ID - requeue a specific failed job\n"
    "/password JOB_ID PASSWORD - unlock encrypted RAR\n"
    "/help - this help\n\n"
    "Private SQLite import/index runs only after /import. No public API."
)


def _human_bytes(n):
    if n is None:
        return "unavailable"
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.2f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024


def _proc_io_bytes():
    """Linux per-process disk I/O counters, not file-system cache traffic."""
    with open("/proc/self/io", encoding="ascii") as fh:
        values = {}
        for line in fh:
            key, sep, value = line.partition(":")
            if sep:
                values[key.strip()] = int(value.strip())
    return values.get("read_bytes", 0), values.get("write_bytes", 0)


def _proc_rss_bytes():
    with open("/proc/self/status", encoding="ascii") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return None


def _sqlite_extra_sizes(path):
    result = {}
    for suffix, name in (("", "DB"), ("-wal", "WAL"), ("-shm", "SHM")):
        try:
            result[name] = Path(str(path) + suffix).stat().st_size
        except OSError:
            result[name] = 0
    return result


def _io_sample(seconds=1.5):
    """Run in a worker thread to avoid blocking the Telegram event loop."""
    t1 = time.monotonic()
    proc_start = os.times()
    try:
        r1, w1 = _proc_io_bytes()
        time.sleep(seconds)
        r2, w2 = _proc_io_bytes()
        elapsed = max(time.monotonic() - t1, 0.01)
        proc_end = os.times()
        cpu_used = ((proc_end.user + proc_end.system) -
                    (proc_start.user + proc_start.system))
        return {"read": max(0, r2-r1) / elapsed,
                "write": max(0, w2-w1) / elapsed,
                "cpu": max(0.0, cpu_used / elapsed * 100.0),
                "rss": _proc_rss_bytes()}
    except (OSError, ValueError):
        return None


def _csv_file_count(job_id):
    if not re.fullmatch(r"[0-9a-f]{12}", job_id or ""):
        return None
    for root in (BASE / job_id, BASE.parent / job_id):
        if root.is_dir():
            return sum(1 for p in root.rglob("*") if p.is_file() and p.suffix.lower() == ".csv")
    return None


def _read_import_stats():
    """Collect low-cost SQLite and OS stats from the worker thread."""
    path = sqlite_db_path(BASE)
    info = sqlite_status(path)
    sizes = _sqlite_extra_sizes(path)
    disk = shutil.disk_usage(BASE)
    job_id = import_active or sqlite_pending_job(path)
    total_files = _csv_file_count(job_id) if job_id else None
    return info, sizes, disk.free, disk.total, job_id, total_files


async def import_status_text():
    info, sizes, free, total, job_id, total_files = await asyncio.to_thread(_read_import_stats)
    if not info.get("exists"):
        return "No SQLite import started. Use /import JOB_ID."

    active = info.get("active")
    registered = info["sources"]
    completed = info["completed_sources"]
    expected = max(registered, total_files) if total_files is not None else registered
    running = bool(import_task and not import_task.done())

    if info["indexed"]:
        phase = "✅ USER ID INDEX READY"
    elif job_id and completed >= expected and expected > 0 and active is None:
        phase = "🔨 BUILDING USER ID INDEX" if running else "⏸ Index pending / paused"
    elif running:
        phase = "📥 IMPORTING CSV"
    else:
        phase = "⏸ Import not currently active"

    lines = [
        f"🗃 SQLite: {phase}",
        f"Job: {job_id or 'none'}",
        f"CSV files: {completed}/{expected}" +
        (f" ({registered} registered)" if expected != registered else ""),
        f"Imported records: {info['imported']:,}",
        f"Invalid IDs: {info['invalid_ids']:,}",
        f"Inconsistent rows: {info['inconsistent_rows']:,}",
        f"Database: {_human_bytes(sizes['DB'])}",
        f"WAL / SHM: {_human_bytes(sizes['WAL'])} / {_human_bytes(sizes['SHM'])}",
        f"Free disk: {_human_bytes(free)} / {_human_bytes(total)}",
    ]
    if active:
        lines += [f"Current CSV: {active[1]}",
                  f"CSV rows scanned: {active[2]:,}"]
    elif not info["indexed"] and completed == expected and expected:
        lines.append("Current step: creating SQLite User ID index")
    lines.append(f"Bot PID: {os.getpid()}")

    sample = await asyncio.to_thread(_io_sample)
    if sample is None:
        lines.append("Bot disk I/O: unavailable (Linux /proc required)")
    else:
        lines += [
            f"Bot CPU usage: {sample['cpu']:.1f}% (sampled)",
            f"Bot RAM (RSS): {_human_bytes(sample['rss'])}",
            f"Bot disk READ: {_human_bytes(sample['read'])}/s",
            f"Bot disk WRITE: {_human_bytes(sample['write'])}/s",
        ]
    lines.append("I/O rates are for the entire bot process, sampled over ~1.5 sec.")
    if not info["indexed"]:
        lines.append("Lookup becomes available once User ID index is ready.")
    return "\n".join(lines)


def import_progress(filename, cursor):
    # Called on import worker's thread; never contact Telegram from this thread.
    log.info("SQLite import progress: %s row %s", filename, cursor)


async def run_import(job_id):
    global import_active
    import_active = job_id
    try:
        await client.send_message(OWNER_ID, f"📥 SQLite import started/resumed for {job_id}. Check /importstatus.")
        result = await asyncio.to_thread(sqlite_import_job, BASE, job_id, import_progress)
        await client.send_message(
            OWNER_ID, f"✅ SQLite import + User ID index completed\n"
            f"Sources: {result['completed_sources']}/{result['sources']}\n"
            f"Indexed rows: {result['imported']:,}\n"
            f"Rows with invalid IDs: {result['invalid_ids']:,}\n"
            f"Rows with inconsistent columns: {result['inconsistent_rows']:,}\n"
            "Use /lookup USER_ID to search privately."
        )
    except Exception as exc:
        log.exception("SQLite import failed for %s", job_id)
        await client.send_message(
            OWNER_ID, f"❌ SQLite import paused/failed for {job_id}: {type(exc).__name__}: "
            f"{str(exc)[:240]}\nSaved progress is preserved. Use /import {job_id} to retry."
        )
    finally:
        import_active = None
        import_requested.discard(job_id)


async def start_import(job_id):
    global import_task
    if import_active is not None or (import_task is not None and not import_task.done()):
        return False
    import_requested.add(job_id)
    import_task = asyncio.create_task(run_import(job_id))
    return True


async def username_index_worker():
    try:
        await client.send_message(OWNER_ID, "🔎 Username index building from existing SQLite. Use /indexstatus.")
        def progress(cursor):
            log.info("Username index scan rowid=%s", cursor)
        result = await asyncio.to_thread(build_username_index, sqlite_db_path(BASE), progress)
        await client.send_message(OWNER_ID, f"✅ Username index ready! Entries: {result['entries']:,}. Use /user @username")
    except Exception as exc:
        log.exception("Username index failed")
        await client.send_message(
            OWNER_ID,
            f"❌ Username index interrupted: {type(exc).__name__}: {str(exc)[:200]}. "
            "Use /usernameindex to resume."
        )


async def username_lookup_private(message, raw_args):
    pieces = raw_args.split()
    if not pieces:
        await message.reply_text("Usage: /user @username [PAGE]")
        return
    try:
        page = int(pieces[1]) if len(pieces)>1 else 1
        result, more = await asyncio.to_thread(lookup_username, sqlite_db_path(BASE), pieces[0], page)
    except Exception as exc:
        await message.reply_text("Username search unavailable: " + str(exc)[:250])
        return
    if not result:
        await message.reply_text("No matching username found.")
        return
    for i, (filename, uid, pairs) in enumerate(result,1):
        lines=[f"🔎 Username: {pieces[0]}",f"User ID: {uid}",f"Source: {filename}",f"Page: {page} · Match {i}"]
        lines += [f"{key}: {value}" for key,value in pairs]
        value="\n".join(lines)
        if len(value)>30000:
            attachment=io.BytesIO(value.encode("utf-8"))
            attachment.name=f"username_record_{page}_{i}.txt"
            await client.send_document(OWNER_ID,attachment,caption="Full username record")
        else:
            for at in range(0,len(value),3500):
                await message.reply_text(value[at:at+3500])
    if more:
        await message.reply_text(f"More: /user {pieces[0]} {page+1}")


async def lookup_private(message, raw_args):
    parts = raw_args.split()
    if not parts:
        await message.reply_text("Usage: /lookup USER_ID [PAGE]")
        return
    number = parts[0]
    try:
        page = int(parts[1]) if len(parts) > 1 else 1
        result, more = await asyncio.to_thread(
            sqlite_lookup, sqlite_db_path(BASE), number, page
        )
    except (ValueError, OSError, Exception) as exc:
        await message.reply_text(f"Lookup unavailable: {str(exc)[:250]}")
        return
    if not result:
        await message.reply_text("No matching record on this page.")
        return
    for i, (filename, pairs, extras) in enumerate(result, start=1):
        lines = [f"🔍 User ID: {number}", f"Source: {filename}", f"Page: {page} · Match {i}"]
        lines.extend(f"{key}: {value}" for key, value in pairs)
        if extras:
            lines.extend(f"extra_{j}: {v}" for j, v in enumerate(extras, start=1))
        message_text = "\n".join(lines)
        # Split long records without silently omitting fields.
        if len(message_text) > 30000:
            # Telegram text messages have a size limit; deliver all fields as a private document.
            document = io.BytesIO(message_text.encode("utf-8"))
            document.name = f"record_{number}_page_{page}_match_{i}.txt"
            await client.send_document(OWNER_ID, document, caption="Full matching record")
            continue
        for start in range(0, len(message_text), 3500):
            await message.reply_text(message_text[start:start+3500])
    if more:
        await message.reply_text(f"More matches: /lookup {number} {page+1}")


@client.on_message(filters.all)
async def message_router(_, message):
    """Single robust handler for private messages; log delivery and failures."""
    global username_task
    sender = getattr(message, "from_user", None)
    user_id = sender.id if sender else None
    chat = getattr(message, "chat", None)
    chat_type = str(getattr(chat, "type", "")).lower()
    if "private" not in chat_type:
        return
    command = ((message.text or "").split(maxsplit=1)[0].split("@")[0].lower()
               if message.text and message.text.startswith("/") else "")
    # Do not log sensitive commands or their arguments.
    log.info("Incoming private update from uid=%s command=%s document=%s",
             user_id, command or "(none)", bool(message.document))
    try:
        if not await authorized(message):
            return
        if command in ("/start", "/help"):
            await message.reply_text(HELP)
        elif command == "/ping":
            await message.reply_text("🏓 Pong! Bot is online and receiving messages.")
        elif command == "/status":
            info = (
                f"📊 Bot online\nActive: {active_id or 'none'}\n"
                f"Queue: {queue.qsize()}\nFree: {shutil.disk_usage(BASE).free / 1024**3:.2f} GiB\n"
                f"Storage: {BASE}"
            )
            if active_id and active_id in known_jobs:
                info += "\n\n" + job_status(known_jobs[active_id])
            await message.reply_text(info)
        elif command == "/list":
            jobs = sorted(known_jobs.values(), key=lambda j: j["created"], reverse=True)[:10]
            await message.reply_text("\n\n".join(job_status(j) for j in jobs)
                                     if jobs else "No uploads yet.")
        elif command == "/audit":
            args = (message.text or "").split(maxsplit=1)
            jid = args[1].strip().lower() if len(args) > 1 else ""
            job = known_jobs.get(jid)
            if not job or job.get("state") != "done" or not output_path(job).is_dir():
                await message.reply_text("Usage: /audit JOB_ID for a completed extraction. Use /list.")
                return
            await message.reply_text("🔎 Checking CSV schema and sample quality...")
            report = await asyncio.to_thread(audit_folder, output_path(job))
            await asyncio.to_thread(write_audit, output_path(job), report)
            await message.reply_text(audit_summary(report))
        elif command == "/import":
            args = (message.text or "").split(maxsplit=1)
            jid = args[1].strip().lower() if len(args) > 1 else ""
            folder = (BASE / jid).resolve()
            if not folder.is_dir():
                folder = (BASE.parent / jid).resolve()
            if (not re.fullmatch(r"[0-9a-f]{12}", jid)
                    or not folder.is_dir()
                    or folder.parent not in (BASE, BASE.parent)):
                await message.reply_text("Usage: /import JOB_ID; extracted folder must exist in DATA_DIR or repository folder.")
                return
            started = await start_import(jid)
            await message.reply_text(
                f"📥 Import queued for {jid}. Use /importstatus to monitor."
                if started else f"Import already running: {import_active or 'starting'}."
            )
        elif command == "/importstatus":
            await message.reply_text(await import_status_text())
        elif command == "/lookup":
            args = (message.text or "").split(maxsplit=1)
            await lookup_private(message, args[1] if len(args) > 1 else "")
        elif command == "/usernameindex":
            if import_active or (import_task and not import_task.done()):
                await message.reply_text("CSV import still running. Start username index after /importstatus shows INDEX READY.")
                return
            if username_task and not username_task.done():
                await message.reply_text("Username index is already building.")
                return
            import_info = await asyncio.to_thread(sqlite_status, sqlite_db_path(BASE))
            if not import_info.get("indexed"):
                await message.reply_text("Finish SQLite import and User ID index first.")
                return
            username_task = asyncio.create_task(username_index_worker())
            await message.reply_text("✅ Username index job started. Use /indexstatus.")
        elif command == "/indexstatus":
            info = await asyncio.to_thread(username_index_status, sqlite_db_path(BASE))
            await message.reply_text(
                f"Username index: {'READY' if info['ready'] else 'NOT READY / BUILDING'}\n"
                f"Indexed usernames: {info['entries']:,}\n"
                f"Scanned through record rowid: {info['cursor']:,}"
            )
        elif command == "/user":
            args = (message.text or "").split(maxsplit=1)
            await username_lookup_private(message, args[1] if len(args)>1 else "")
        elif command == "/pause":
            if not active_id or known_jobs[active_id]["state"] != "downloading":
                await message.reply_text("No active download to pause.")
            else:
                pause_requested.add(active_id)
                await message.reply_text(f"⏸ Pausing {active_id} at next chunk boundary.")
        elif command == "/resume":
            count = 0
            for job in sorted(known_jobs.values(), key=lambda j: j["created"]):
                if job["state"] in ("paused", "failed"):
                    count += bool(queue_job(job))
            await message.reply_text(f"Queued {count} paused/failed job(s).")
        elif command == "/retry":
            args = (message.text or "").split(maxsplit=1)
            jid = args[1].lower().strip() if len(args) > 1 else ""
            job = known_jobs.get(jid)
            if not job:
                await message.reply_text("Usage: /retry JOB_ID (see /list)")
            elif job["state"] not in ("paused", "failed", "awaiting_password"):
                await message.reply_text("Cannot retry job in state " + job["state"])
            else:
                await message.reply_text("✅ Queued." if queue_job(job) else "Already queued.")
        elif command == "/password":
            parts = (message.text or "").split(maxsplit=2)
            if len(parts) < 3:
                await message.reply_text("Usage: /password JOB_ID PASSWORD")
                return
            job = known_jobs.get(parts[1].lower())
            if not job or job["state"] not in ("awaiting_password", "failed", "paused"):
                await message.reply_text("Job not found or not waiting for password; see /list.")
                return
            if not parts[2] or len(parts[2]) > 512:
                await message.reply_text("Password must contain 1–512 characters.")
                return
            set_password(job, parts[2])
            deleted = False
            try:
                await message.delete(revoke=True)
                deleted = True
            except Exception:
                log.warning("Could not delete password message; user should delete it.")
            await client.send_message(
                OWNER_ID,
                f"🔐 Password stored and extraction queued: {job['id']}."
                + ("" if deleted else " Delete your password message manually.")
            )
            queue_job(job)
        elif message.document:
            document = message.document
            filename = document.file_name or ""
            if not filename.lower().endswith(".rar"):
                await message.reply_text("Send a .rar archive as a Telegram document.")
                return
            if not document.file_size:
                await message.reply_text("Telegram didn't report document size.")
                return
            if any(j["message_id"] == message.id for j in known_jobs.values()):
                await message.reply_text("This message is already queued.")
                return
            job = {
                "id": uuid.uuid4().hex[:12],
                "name": safe_name(filename) if safe_name(filename).lower().endswith(".rar")
                        else safe_name(filename) + ".rar",
                "message_id": message.id,
                "size": document.file_size,
                "state": "queued",
                "created": int(time.time())
            }
            save_job(job)
            queue_job(job)
            await message.reply_text(
                f"✅ File queued: {job['name']}\nID: {job['id']}\n"
                f"Size: {bytes_human(job['size'])}\nResumable download enabled."
            )
    except Exception:
        log.exception("Message handler failed; uid=%s command=%s", user_id, command)
        try:
            await message.reply_text("❌ Handler error. Check server console logs.")
        except Exception:
            log.exception("Could not send handler error to Telegram")


async def main():
    global worker_task
    load_jobs()
    await client.start()
    me = await client.get_me()
    log.info("Started @%s; %s storage; %d saved jobs; OWNER_ID=%s", me.username, BASE, len(known_jobs), OWNER_ID)
    try:
        await client.send_message(OWNER_ID, "✅ Database bot started. Send /ping to test responses.")
        log.info("Startup test message sent to OWNER_ID")
    except Exception:
        log.exception("Startup message to OWNER_ID failed; verify OWNER_ID or start the bot in Telegram")
    worker_task = asyncio.create_task(worker())
    # Resume a saved SQLite import interrupted by a reboot (no data replay).
    try:
        pending = await asyncio.to_thread(sqlite_pending_job, sqlite_db_path(BASE))
        if pending and re.fullmatch(r"[0-9a-f]{12}", pending):
            await start_import(pending)
    except Exception:
        log.exception("Could not recover pending SQLite import")
    # Recover interrupted transfers. Extraction can be rerun from completed .part.
    for job in sorted(known_jobs.values(), key=lambda j: j["created"]):
        if job["state"] in ("queued", "downloading", "extracting"):
            queue_job(job)
    try:
        await idle()
    finally:
        if import_task and not import_task.done():
            log.warning('Import worker will resume from committed batches on next start')
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await client.stop()


if __name__ == "__main__":
    try:
        # Use the same loop as the module-level Client and job queue.
        client.run(main())
    except KeyboardInterrupt:
        pass
