#!/usr/bin/env python3
"""Owner-only Telegram RAR archive receiver and safe extractor."""
import asyncio
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
from telethon import TelegramClient, events

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("db-upload")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
BASE_DIR = Path(os.environ.get("DATA_DIR", "./database_uploads")).expanduser().resolve()
MAX_EXTRACT_GB = float(os.environ.get("MAX_EXTRACT_GB", "20"))
MAX_FILES = int(os.environ.get("MAX_FILES", "100000"))
KEEP_RAR = os.environ.get("KEEP_RAR", "false").strip().lower() == "true"
MAX_EXTRACT_BYTES = int(MAX_EXTRACT_GB * 1024**3)
CHUNK = 1024 * 1024
LOCK = asyncio.Lock()

if not all((BOT_TOKEN, API_ID, API_HASH, OWNER_ID)):
    raise SystemExit("Set BOT_TOKEN, API_ID, API_HASH and OWNER_ID in .env")
if MAX_EXTRACT_BYTES <= 0 or MAX_FILES <= 0:
    raise SystemExit("MAX_EXTRACT_GB and MAX_FILES must be positive")

BASE_DIR.mkdir(parents=True, exist_ok=True)
(BASE_DIR / ".incoming").mkdir(exist_ok=True)
client = TelegramClient("upload_bot_session", API_ID, API_HASH)


def safe_filename(name: str) -> str:
    name = Path(name.replace("\\", "/")).name
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return name[:120] or "database.rar"


def validate_member(info):
    name = info.filename.replace("\\", "/")
    path = PurePosixPath(name)
    if not name or name.startswith("/") or re.match(r"^[A-Za-z]:", name):
        raise ValueError("Archive contains an absolute path")
    if any(part in ("", ".", "..") for part in name.split("/")):
        raise ValueError("Archive contains unsafe path components")
    if len(path.parts) > 30 or len(name) > 1000:
        raise ValueError("Archive contains an overly long path")
    if hasattr(info, "is_symlink") and info.is_symlink():
        raise ValueError("Archive contains a symlink")
    mode = getattr(info, "mode", None)
    if isinstance(mode, int) and mode and stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR):
        raise ValueError("Archive contains a special filesystem entry")
    return path


def extract_rar(archive: Path, output: Path) -> tuple[int, int]:
    """Validate the entire archive before writing; extract via file streams."""
    output.mkdir(parents=True, exist_ok=False)
    try:
        with rarfile.RarFile(str(archive)) as rf:
            if rf.needs_password():
                raise ValueError("Password-protected RAR is not supported yet")
            members = rf.infolist()
            if len(members) > MAX_FILES:
                raise ValueError(f"Too many entries (limit {MAX_FILES})")
            total = 0
            seen = set()
            for item in members:
                path = validate_member(item)
                key = str(path).casefold()
                if key in seen:
                    raise ValueError("Archive has duplicate file names")
                seen.add(key)
                if not item.isdir():
                    total += item.file_size
                    if total > MAX_EXTRACT_BYTES:
                        raise ValueError(f"Uncompressed data exceeds {MAX_EXTRACT_GB:g} GB")
            # Allow room for archive + extracted data; never write on a nearly full disk.
            if shutil.disk_usage(output).free < total + 100 * 1024**2:
                raise ValueError("Not enough free disk space for extraction")
            extracted = 0
            actual_total = 0
            for item in members:
                target = output.joinpath(*validate_member(item).parts)
                if item.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                written = 0
                with rf.open(item) as source, target.open("xb") as dest:
                    while True:
                        chunk = source.read(CHUNK)
                        if not chunk:
                            break
                        written += len(chunk)
                        actual_total += len(chunk)
                        if written > item.file_size or actual_total > MAX_EXTRACT_BYTES:
                            raise ValueError("Archive exceeds its declared extraction size")
                        dest.write(chunk)
                if written != item.file_size:
                    raise ValueError("Archive member length mismatch")
                extracted += 1
            return extracted, actual_total
    except BaseException:
        shutil.rmtree(output, ignore_errors=True)
        raise


def summarize() -> str:
    folders = sorted(
        (p for p in BASE_DIR.iterdir() if p.is_dir() and not p.name.startswith(".")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not folders:
        return "No extracted uploads yet."
    lines = ["Extracted uploads (latest first):"]
    for folder in folders[:15]:
        count = sum(1 for f in folder.rglob("*") if f.is_file())
        lines.append(f"• {folder.name}: {count} files")
    if len(folders) > 15:
        lines.append(f"... and {len(folders)-15} more")
    return "\n".join(lines)


async def owner_only(event) -> bool:
    if event.sender_id != OWNER_ID or not event.is_private:
        if event.is_private:
            await event.reply("⛔ Access denied.")
        return False
    return True


@client.on(events.NewMessage(pattern=r"^/(start|help)(?:@\w+)?$", incoming=True))
async def start(event):
    if not await owner_only(event):
        return
    await event.reply(
        "📦 Database Upload Bot\n\n"
        "Send a .rar archive as a Telegram DOCUMENT (not as a link).\n"
        "I will download it on this server and extract it into DATA_DIR.\n\n"
        "/list - show extracted uploads\n"
        "/status - show current upload state\n"
        "/help - show this message\n\n"
        "This phase stores/extracts files only; it does not import them into an SQL/Mongo database."
    )


@client.on(events.NewMessage(pattern=r"^/status(?:@\w+)?$", incoming=True))
async def status(event):
    if not await owner_only(event):
        return
    await event.reply(
        f"Server storage: {BASE_DIR}\n"
        f"Job: {'BUSY' if LOCK.locked() else 'IDLE'}\n"
        f"Free disk: {shutil.disk_usage(BASE_DIR).free / 1024**3:.2f} GiB"
    )


@client.on(events.NewMessage(pattern=r"^/list(?:@\w+)?$", incoming=True))
async def list_uploads(event):
    if not await owner_only(event):
        return
    await event.reply(await asyncio.to_thread(summarize))


@client.on(events.NewMessage(func=lambda e: e.is_private and bool(e.message.file), incoming=True))
async def receive_archive(event):
    if not await owner_only(event):
        return
    filename = event.file.name or ""
    if not filename.lower().endswith(".rar"):
        await event.reply("Please send a .rar file as a document.")
        return
    if LOCK.locked():
        await event.reply("⚠️ An upload is already running. Send the next archive when it finishes.")
        return

    async with LOCK:
        job = uuid.uuid4().hex[:8]
        archive_name = safe_filename(filename)
        temp = BASE_DIR / ".incoming" / f"{job}_{archive_name}"
        target = BASE_DIR / f"{time.strftime('%Y%m%d_%H%M%S')}_{job}_{archive_name[:-4]}"
        msg = await event.reply("⬇️ Downloading RAR from Telegram...")
        last_update = [0.0]

        async def progress(current, total):
            now = time.monotonic()
            if now - last_update[0] < 8 and current < total:
                return
            last_update[0] = now
            percent = current * 100 / total if total else 0
            try:
                await msg.edit(f"⬇️ Downloading: {percent:.1f}% ({current / 1024**2:.1f} MiB)")
            except Exception:
                pass

        try:
            await event.message.download_media(file=str(temp), progress_callback=progress)
            if not temp.is_file() or temp.stat().st_size == 0:
                raise RuntimeError("Telegram download produced no file")
            await msg.edit("📂 Download complete. Validating and extracting RAR...")
            file_count, bytes_extracted = await asyncio.to_thread(extract_rar, temp, target)
            if KEEP_RAR:
                archive_dir = BASE_DIR / ".archives"
                archive_dir.mkdir(exist_ok=True)
                shutil.move(str(temp), str(archive_dir / f"{job}_{archive_name}"))
            await msg.edit(
                "✅ Upload and extraction completed!\n"
                f"Archive: {archive_name}\n"
                f"Extracted files: {file_count}\n"
                f"Unpacked size: {bytes_extracted / 1024**2:.1f} MiB\n"
                f"Server folder: {target}\n\n"
                "Note: Files are stored, not imported into a database engine."
            )
            log.info("Uploaded %s into %s", archive_name, target)
        except Exception as exc:
            log.exception("Upload failed")
            shutil.rmtree(target, ignore_errors=True)
            try:
                await msg.edit(f"❌ Upload/extraction failed: {str(exc)[:300]}")
            except Exception:
                await event.reply("❌ Upload/extraction failed. Check server logs.")
        finally:
            temp.unlink(missing_ok=True)


async def main():
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    log.info("Bot @%s started; storage=%s", me.username, BASE_DIR)
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
