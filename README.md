# Smart Telegram RAR database uploader

An owner-only Telegram bot that downloads database archives to persistent storage using **1 MiB Pyrofork stream chunks**, safely resumes after disconnect/restart, and extracts the RAR without modifying any existing database.

## Features

- **Resume after reboot**: saves original Telegram message ID, expected file length, state and partial `.part` file under `DATA_DIR`. Restart continues from the last complete chunk, not 0%.
- Automatic retry (exponential backoff); stalled/failed jobs kept on disk for manual retry.
- Sequential queue so multiple incoming RAR files don't download simultaneously; queued jobs survive restart.
- Telegram progress message displays percentage, downloaded MiB, current MiB/s and ETA. Telegram edits are throttled to avoid flood limits.
- Owner-only private chat. One instance per storage folder. `/pause`, `/resume`, `/retry`, `/status`, `/list`.
- Safe extraction into a staging directory; path traversal and special file entries rejected, with file-count/size and free-space limits.
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

If the archive is encrypted, the bot changes the job to `awaiting_password` and keeps its complete downloaded RAR in `.incoming`. Find the job ID with `/list`, then send `/password JOB_ID YOUR_PASSWORD` in the private bot chat. It attempts to delete that Telegram message immediately and stores the password in `DATA_DIR/.secrets/JOB_ID.password` (0600 permissions) so a restart during retry will work. It removes the file after successful extraction. **Telegram chat is not a secret vault:** deletion is best effort; avoid reusing important passwords. Telegram service, backups, or notifications may retain the original command. You may also delete `DATA_DIR/.secrets/JOB_ID.password` after use. Failed extraction retains the downloaded archive for another retry.

Older initial builds removed the downloaded RAR on extraction failure; uploads failed using those versions may need to be sent again. Password is never echoed in bot progress messages.
