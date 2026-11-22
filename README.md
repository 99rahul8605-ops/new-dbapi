# Smart Telegram RAR database uploader

An owner-only Telegram bot that downloads database archives to persistent storage using **1 MiB Pyrofork stream chunks**, safely resumes after disconnect/restart, and extracts the RAR without modifying any existing database.

## Features

- **Resume after reboot**: saves original Telegram message ID, expected file length, state and partial `.part` file under `DATA_DIR`. Restart continues from the last complete chunk, not 0%.
- Automatic retry (exponential backoff); stalled/failed jobs kept on disk for manual retry.
- Sequential queue so multiple incoming RAR files don't download simultaneously; queued jobs survive restart.
- Telegram progress message displays percentage, downloaded MiB, current MiB/s and ETA. Telegram edits are throttled to avoid flood limits.
- Owner-only private chat. One instance per storage folder. `/pause`, `/resume`, `/retry`, `/status`, `/list`.
- Safe extraction using system `unar` and `lsar` into a staging directory; archive listing checks paths, file-count/size and available disk before extraction.
- On successful extraction removes the downloaded archive unless `KEEP_RAR=true`. Failed extraction **retains** the archive for retry.

### Limitations

Supports **single-volume encrypted or unencrypted .rar documents**. Encrypted RARs need the correct owner-provided password. Multipart RARs are not supported. A Telegram message deleted by its sender or unavailable to the bot cannot be resumed. Upload progress from your phone to Telegram is controlled by Telegram; this bot tracks **Telegram-to-VPS downloads**. The server must have stable persistent storage. Telegram/network may limit transfer speeds; `tgcrypto-pyrofork` speeds up cryptography but does not guarantee download speed. Extraction size is constrained by available disk and `MAX_EXTRACT_GB`. Resumption uses complete 1 MiB offsets and may re-fetch up to one partial chunk. It does not verify a cryptographic hash of the entire original Telegram archive.

This is **storage/extraction only**: it does not import SQL files, MongoDB dumps, or SQLite data into a database service.

## Deploy on Ubuntu / Debian VPS

```bash
apt update
apt install -y python3 python3-venv git unar screen
# Existing clone: cd ~/new-dbapi && git pull origin main
# First install only:
git clone https://github.com/99rahul8605-ops/new-dbapi.git
cd new-dbapi
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
chmod 600 .env
```

Fill `BOT_TOKEN`, `API_ID`, `API_HASH`, `OWNER_ID`, `DATA_DIR` in `.env`. Choose an **absolute**, persistent DATA_DIR; keep it unchanged on upgrades (e.g. `/root/db-storage`).

```bash
screen -dmS db bash -lc 'cd ~/new-dbapi && exec .venv/bin/python -u bot.py >> bot.log 2>&1'
screen -ls
tail -f ~/new-dbapi/bot.log
```

If the bot already runs in the `db` screen, stop it *before* restarting with new code:

```bash
cd ~/new-dbapi
screen -S db -X quit
sleep 2
git pull origin main
source .venv/bin/activate
pip uninstall -y pyrogram telethon cryptg tgcrypto 2>/dev/null || true
pip install -r requirements.txt
screen -dmS db bash -lc 'cd ~/new-dbapi && exec .venv/bin/python -u bot.py >> bot.log 2>&1'
screen -ls
tail -n 30 bot.log
```

**Important migration note:** Downloads begun with the **first single-file download version** did not have saved Telegram message metadata; the new code cannot reliably resume them. Downloads received by the **new version** will resume after subsequent restarts. An interrupted extraction may need retry; the original RAR is retained until success.

## Bot commands

| Command | Meaning |
|---|---|
| `/start`, `/help` | Help |
| `/status` | Active transfer, queue, free disk |
| `/list` | Last 10 jobs, IDs and states |
| `/pause` | Pause active download after next chunk |
| `/resume` | Requeue all paused/failed jobs |
| `/retry JOB_ID` | Retry one paused/failed job from saved bytes |
| `/password JOB_ID PASSWORD` | Give password for a failed or password-locked RAR, then retry extraction without downloading again |

Send a .rar document as a private Telegram message to the bot. Incoming documents queue automatically. No public upload endpoint is exposed.

## Security

Run as a restricted Linux user, ideally with disk quotas and filesystem protections. Archive content is not executed. Do not put `.env`, Telegram session files, or `DATA_DIR` into Git. The bot is intentionally not a remote shell; users cannot provide arbitrary filesystem paths or commands. If source data is untrusted, use OS-level sandboxing as an additional defense. Verify free disk space before very large transfers.

## Pyrofork migration (November 2026)

The bot uses **Pyrofork** (a Pyrogram-compatible maintained fork) and **TgCrypto-pyrofork**. It imports from `pyrogram` but the installed PyPI distribution is `pyrofork`. Python session state is stored in `upload_bot_pyrofork_session.session` rather than the old Telethon session. The `DATA_DIR/.jobs/*.json` and `DATA_DIR/.incoming/*.part` formats stay compatible with the previous resumable Telethon release. On the first Pyrofork startup, the .part files are rounded back to a full 1 MiB boundary, so at most the last partial MiB is re-downloaded. Do **not** delete DATA_DIR or change it when migrating.

Wait for a currently running download to finish before stopping the previous screen session. Pull and install from the README's restart instructions afterward. To verify installed backend: `python -c 'import pyrogram; print(pyrogram.__version__)'`. The migration has not been live-tested on your VPS yet; watch `bot.log` after first start.

**Safety:** Pyrofork's high-level `download_media` has had a path-traversal advisory. This project does **not** use that high-level file writer: it streams bytes directly into an internally generated filename, without trusting Telegram-provided paths.

## Password-protected RAR

If the archive is encrypted or password is wrong, the bot changes the job to `awaiting_password` and keeps its complete downloaded RAR in `.incoming`. Find the job ID with `/list`, then send `/password JOB_ID YOUR_PASSWORD` in the private bot chat. It attempts to delete that Telegram message immediately and stores the password in `DATA_DIR/.secrets/JOB_ID.password` (0600 permissions) so a restart during retry will work. It removes the file after successful extraction. **Telegram chat is not a secret vault:** deletion is best effort; avoid reusing important passwords. Telegram service, backups, or notifications may retain the original command. You may also delete `DATA_DIR/.secrets/JOB_ID.password` after use. Failed extraction retains the downloaded archive for another retry.

Older initial builds removed the downloaded RAR on extraction failure; uploads failed using those versions may need to be sent again. Password is never echoed in bot progress messages.

## Extraction fix (2026-11-22)

The Python `rarfile` stream extractor was replaced by the same `unar` executable used by the server's successful manual test. `lsar -json` validates paths, file count and uncompressed size first. `unar` extracts to a private staging directory; only a successful extraction is moved to `DATA_DIR/JOB_ID`. On failure the complete `.incoming/JOB_ID.part` is retained. Install `unar` (includes `lsar`) with `apt install -y unar`.

For an already-downloaded failed archive, keep `DATA_DIR` unchanged and use `/retry JOB_ID` after upgrading. If the job is waiting for a password, send `/password JOB_ID ACTUAL_PASSWORD` instead. No 3.5 GiB re-download should be required when the saved partial file is complete.

**Password privacy caveat:** The `unar` and `lsar` CLI interfaces accept password options as command arguments. A sufficiently privileged local OS user may briefly see them in process listings; use a dedicated single-user VPS, restrict other accounts, and use passwords that are not reused elsewhere. The bot never prints the password to its own logs.

## Automatic CSV audit after extraction

After each successful RAR extraction the bot automatically samples up to 1,000 data rows per `.csv` file. It detects per-file delimiter (comma, semicolon, pipe, tab), maps candidate ID headers (`Telegram ID`, `User ID`, `User_ID`, `account_id`, `id`), counts numeric ID values, and reports inconsistent sampled row widths. Results go to `DATA_DIR/JOB_ID/_csv_audit.json`, containing aggregate counts and column metadata, **not personal data rows**. The bot sends a brief summary to the owner. Use `/audit JOB_ID` to rerun the same check on an already completed extraction; extraction and audit are separate, so audit failure does not invalidate extraction. This is a sampling step, not a full-file integrity scan or database import. No SQLite tables or per-user search indices are created by this feature.

**Processing checklist for future phases:** archive reception → resumable download → safe extraction → automatic CSV format/schema audit → aggregate validation report → only after data authorization, design suitable data storage/import and optional indexing. Original archives and CSVs should not be overwritten as part of auditing.

## Private SQLite importer and User ID index

The owner can import extracted CSVs into a private SQLite database, using the **complete original CSV row**, including all source columns and duplicate IDs. The SQLite DB lives at `DATA_DIR/.private_index/records.sqlite3`. It is not exposed through any HTTP API.

- `/import JOB_ID` starts the streaming import for extracted job folder `DATA_DIR/JOB_ID`. Legacy manually extracted folders at `DATA_DIR/../JOB_ID` are also supported, so you do not have to move 24 GB of files.
- `/importstatus` reports completed files, row count, invalid ID count, inconsistent column count, indexed/not-indexed state and DB size.
- `/lookup USER_ID` privately shows up to three matching records at a time, preserving source column names and field values; use `/lookup USER_ID 2` for the next page. Very long records are sent as a private text document.
- Index creation runs after initial import finishes. Lookup will not claim readiness until index creation completes.
- Every 10,000 parsed CSV records are committed with a stored cursor. After a bot reboot, the job resumes from committed progress. A crashed import skips previously processed rows, so resuming a very large CSV may take time but does not duplicate committed records.
- CSV files are read-only. No data is automatically deleted, deduplicated or overwritten.
- Only the configured Telegram owner is authorized; keep the bot token, owner account and VPS secure. Telegram chats are not a substitute for encrypted-at-rest storage.
- Data is imported only on explicit `/import`, not automatically from the archive upload. Each CSV's own delimiter and ID column are detected independently.

### To import your previously manually extracted archive

After deploying the latest bot, send:

```text
/import ac7e843a2ac2
/importstatus
```

Wait until `INDEX READY`, then `/lookup 123456789` (replace with an ID you are authorized to view). The bot should find the already extracted files in `/new-dbapi/ac7e843a2ac2` if `DATA_DIR=/new-dbapi/database_uploads`. The full dataset may take considerable CPU and disk I/O time to process. Check disk space before starting. The CSV text decoder uses replacement for invalid UTF-8 bytes; originals remain unchanged for recovery or reimport if the encoding needs refinement.
