# Telegram Database RAR Upload Bot

Owner-only Telegram bot that downloads RAR archives directly to your server using Telegram MTProto, validates archive contents, and safely extracts them into a persistent folder.

**Phase 1 only:** Save and extract files. It does not automatically import SQLite, SQL dumps, MongoDB exports, or other formats into a running database.

## Requirements

- Linux VPS with persistent disk space
- Python 3.10+
- Telegram bot token (@BotFather)
- Telegram API_ID / API_HASH from https://my.telegram.org
- Your numeric Telegram user ID
- A RAR extractor backend, e.g. `unar` or `unrar`

## Install on Ubuntu/Debian

```bash
sudo apt update
sudo apt install -y python3 python3-venv unar
git clone https://github.com/99rahul8605-ops/new-dbapi.git
cd new-dbapi
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
python bot.py
```

Fill in `BOT_TOKEN`, `API_ID`, `API_HASH`, and `OWNER_ID` in `.env` first. Configure `DATA_DIR` with an absolute path on a persistent volume (e.g. `/root/db-storage`). Protect .env with `chmod 600 .env`.

For a persistent systemd service, run `python bot.py` under a service manager with `Restart=always` and the working directory set to this repo. Do not run multiple bot instances using the same session file.

## Telegram commands

- `/start` / `/help`: instructions
- `/status`: busy/idle and free disk
- `/list`: latest 15 extracted uploads
- Send one **.rar document** in the bot's private chat to start an upload

Each upload is stored in a unique folder inside `DATA_DIR`, such as `database_uploads/20261122_150000_ab12cd34_backup/`. Uploads run one at a time. On successful extraction the downloaded RAR is removed unless `KEEP_RAR=true`.

## Limits and safety

- Sender must be the configured owner in a private chat.
- Rejects archive traversal paths, symlinks/special entries, duplicates, too many entries, and archives exceeding `MAX_EXTRACT_GB` of declared or actual uncompressed data.
- Verifies available disk space before extraction and cleans failed extractions.
- Password-protected RARs and multipart RAR sets are **not supported** in this version.
- Large Telegram files are received over MTProto. Actual upload limits depend on Telegram, local disk, and connectivity.
- Untrusted archive extraction is never risk-free; prefer running this bot as a dedicated restricted Linux user and use a storage volume with no execute permission.
- Extracted folders remain on the server until you manually delete them. No database is replaced or modified by this bot.

## Future phase

Once the RAR contents and database engine are known, you can add validated import/restore commands with confirmation and backups.
