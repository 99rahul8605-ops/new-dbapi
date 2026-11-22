"""Resumable, bounded-memory CSV -> SQLite importer for private owner access.

Each row is retained verbatim as JSON positional fields, referenced by source
file's complete CSV header. No deduplication or silent dropping of CSV records.
"""
import csv
import json
import os
import sqlite3
import time
from pathlib import Path

ID_HEADERS = {"telegram id", "user id", "user_id", "account_id", "id"}
DELIMITERS = (",", ";", "|", "\t")
BATCH = 10000
csv.field_size_limit(10 * 1024 * 1024)


def db_path(data_dir):
    return Path(data_dir) / ".private_index" / "records.sqlite3"


def connect(path, readonly=False):
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=15)
    else:
        conn = sqlite3.connect(str(path), timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA cache_size=-65536")
        conn.execute("PRAGMA temp_store=FILE")
    return conn


def create_schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS sources (
        id INTEGER PRIMARY KEY,
        job_id TEXT NOT NULL,
        filename TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        mtime_ns INTEGER NOT NULL,
        delimiter TEXT NOT NULL,
        id_pos INTEGER NOT NULL,
        columns_json TEXT NOT NULL,
        cursor_row INTEGER NOT NULL DEFAULT 0,
        imported INTEGER NOT NULL DEFAULT 0,
        invalid_ids INTEGER NOT NULL DEFAULT 0,
        inconsistent INTEGER NOT NULL DEFAULT 0,
        done INTEGER NOT NULL DEFAULT 0,
        UNIQUE(job_id, filename)
    );
    CREATE TABLE IF NOT EXISTS records (
        source_id INTEGER NOT NULL,
        line_number INTEGER NOT NULL,
        user_id TEXT NOT NULL,
        fields_json TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)
    db.commit()


def row_schema(path):
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        first = fh.readline()
        delimiter = max(DELIMITERS, key=first.count)
        fh.seek(0)
        headers = next(csv.reader(fh, delimiter=delimiter))
    position = next((i for i, col in enumerate(headers)
                     if col.strip().lower() in ID_HEADERS), None)
    return delimiter, headers, position


def status(path):
    if not Path(path).is_file():
        return {"exists": False}
    with connect(path, readonly=True) as db:
        sources = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(done),0), COALESCE(SUM(imported),0),"
            "COALESCE(SUM(invalid_ids),0), COALESCE(SUM(inconsistent),0) FROM sources"
        ).fetchone()
        index_row = db.execute(
            "SELECT value FROM meta WHERE key='index_ready'"
        ).fetchone()
        active = db.execute(
            "SELECT job_id, filename, cursor_row FROM sources WHERE done=0 ORDER BY id LIMIT 1"
        ).fetchone()
        return {
            "exists": True,
            "sources": sources[0],
            "completed_sources": sources[1],
            "imported": sources[2],
            "invalid_ids": sources[3],
            "inconsistent_rows": sources[4],
            "indexed": bool(index_row and index_row[0] == "1"),
            "active": active,
            "size_gib": round(Path(path).stat().st_size / 1024**3, 2),
        }


def import_job(data_dir, job_id, progress=None):
    root = Path(data_dir).resolve()
    source_root = (root / job_id).resolve()
    if not source_root.is_dir():
        # Support historical archives extracted manually in the repo working dir.
        source_root = (root.parent / job_id).resolve()
    if not source_root.is_dir() or source_root.parent not in (root, root.parent):
        raise ValueError("No extracted job folder found in DATA_DIR or its parent")
    database = db_path(root)
    database.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(database.parent, 0o700)
    paths = sorted(p for p in source_root.rglob("*") if p.is_file() and p.suffix.lower() == ".csv")
    if not paths:
        raise ValueError("No CSV files were found")
    with connect(database) as db:
        create_schema(db)
        db.execute(
            "INSERT INTO meta(key,value) VALUES('active_import_job',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (job_id,)
        )
        db.commit()
        # Hide incomplete imports from lookup until the index is fully ready.
        db.execute("INSERT INTO meta(key,value) VALUES('index_ready','0') "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        db.commit()
        # Only build the index after import finishes; subsequent imports will
        # insert into an existing index incrementally.
        for file in paths:
            rel = file.relative_to(source_root).as_posix()
            stat = file.stat()
            entry = db.execute(
                "SELECT id, bytes, mtime_ns, delimiter, id_pos, columns_json,"
                "cursor_row, done FROM sources WHERE job_id=? AND filename=?",
                (job_id, rel)
            ).fetchone()
            if entry:
                sid, size, mtime, delimiter, id_pos, header_text, cursor, done = entry
                if (stat.st_size, stat.st_mtime_ns) != (size, mtime):
                    raise ValueError("Source CSV changed after import started: " + rel)
                headers = json.loads(header_text)
                if done:
                    continue
            else:
                delimiter, headers, id_pos = row_schema(file)
                if id_pos is None:
                    raise ValueError("No User ID column in " + rel)
                db.execute(
                    "INSERT INTO sources(job_id,filename,bytes,mtime_ns,delimiter,id_pos,columns_json)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (job_id, rel, stat.st_size, stat.st_mtime_ns, delimiter, id_pos,
                     json.dumps(headers, ensure_ascii=False))
                )
                db.commit()
                sid = db.execute(
                    "SELECT id FROM sources WHERE job_id=? AND filename=?", (job_id, rel)
                ).fetchone()[0]
                cursor = 0
            with file.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
                reader = csv.reader(fh, delimiter=delimiter)
                next(reader)
                # Skip records already committed before a restart. This is
                # sequential scanning without loading skipped records into RAM.
                for _ in range(cursor):
                    try:
                        next(reader)
                    except StopIteration as exc:
                        raise ValueError("CSV shortened during resume: " + rel) from exc
                batch = []
                imported = invalid = inconsistent = 0
                last_report = time.monotonic()
                for row in reader:
                    cursor += 1
                    if len(row) != len(headers):
                        inconsistent += 1
                    raw_id = row[id_pos].strip() if len(row) > id_pos else ""
                    if raw_id.isascii() and raw_id.isdecimal():
                        # Keep original row and all columns. Different files may
                        # have duplicate User IDs and that is intentional.
                        batch.append((
                            sid, cursor, raw_id, json.dumps(row, ensure_ascii=False)
                        ))
                        imported += 1
                    else:
                        invalid += 1
                    if cursor % BATCH == 0:
                        _commit_batch(db, batch, sid, cursor, imported, invalid, inconsistent)
                        batch.clear()
                        imported = invalid = inconsistent = 0
                        if progress and time.monotonic() - last_report > 30:
                            progress(rel, cursor)
                            last_report = time.monotonic()
                _commit_batch(db, batch, sid, cursor, imported, invalid, inconsistent, done=1)
            if progress:
                progress(rel, cursor)
        # Index is built once. Re-running after a crash during index build is safe.
        db.execute("CREATE INDEX IF NOT EXISTS idx_records_user_id ON records(user_id)")
        db.execute("INSERT INTO meta(key,value) VALUES('index_ready','1')"
                   " ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        db.execute("DELETE FROM meta WHERE key='active_import_job'")
        db.commit()
    return status(database)


def _commit_batch(db, batch, sid, cursor, imported, invalid, inconsistent, done=0):
    with db:
        if batch:
            db.executemany(
                "INSERT INTO records(source_id,line_number,user_id,fields_json)"
                " VALUES (?,?,?,?)", batch
            )
        db.execute(
            "UPDATE sources SET cursor_row=?, imported=imported+?, "
            "invalid_ids=invalid_ids+?, inconsistent=inconsistent+?,"
            "done=CASE WHEN ?=1 THEN 1 ELSE done END WHERE id=?",
            (cursor, imported, invalid, inconsistent, done, sid)
        )


def lookup(path, user_id, page=1, page_size=3):
    if not user_id.isascii() or not user_id.isdecimal() or len(user_id) > 20:
        raise ValueError("Provide a numeric Telegram User ID")
    if page < 1 or page > 10000:
        raise ValueError("Invalid page number")
    with connect(path, readonly=True) as db:
        indexed = db.execute(
            "SELECT value FROM meta WHERE key='index_ready'"
        ).fetchone()
        if not indexed or indexed[0] != "1":
            raise ValueError("Index not ready; wait for /importstatus")
        found = db.execute(
            "SELECT s.filename, s.columns_json, r.fields_json "
            "FROM records r JOIN sources s ON s.id=r.source_id "
            "WHERE r.user_id=? LIMIT ? OFFSET ?",
            (user_id, page_size+1, (page-1)*page_size)
        ).fetchall()
    result = []
    for filename, header_json, fields_json in found[:page_size]:
        columns = json.loads(header_json)
        values = json.loads(fields_json)
        result.append((filename, [
            (name if name.strip() else f"column_{i+1}", v)
            for i, (name, v) in enumerate(zip(columns, values))
        ], values[len(columns):]))
    return result, len(found) > page_size


def pending_job(path):
    if not Path(path).is_file():
        return None
    with connect(path, readonly=True) as db:
        row = db.execute(
            "SELECT value FROM meta WHERE key='active_import_job'"
        ).fetchone()
        return row[0] if row else None


def _username_position(headers):
    for index, header in enumerate(headers):
        normalized = str(header).strip().casefold().replace("_", " ").replace("-", " ")
        if normalized in ("username", "user name", "никнейм"):
            return index
    return None


def username_index_status(path):
    if not Path(path).is_file():
        return {"ready": False, "cursor": 0, "entries": 0}
    with connect(path, readonly=True) as db:
        ready = db.execute("SELECT value FROM meta WHERE key='username_index_ready'").fetchone()
        cursor = db.execute("SELECT value FROM meta WHERE key='username_index_cursor'").fetchone()
        table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='usernames'").fetchone()
        total = db.execute("SELECT COUNT(*) FROM usernames").fetchone()[0] if table else 0
    return {"ready": bool(ready and ready[0] == '1'), "cursor": int(cursor[0]) if cursor else 0, "entries": total}


def build_username_index(path, progress=None):
    """Build/resume index from existing SQLite rows, without rereading original CSVs."""
    path = Path(path)
    if not path.is_file():
        raise ValueError("SQLite DB not found. Run /import first")
    with connect(path) as db:
        db.execute("CREATE TABLE IF NOT EXISTS usernames (record_rowid INTEGER PRIMARY KEY, username_norm TEXT NOT NULL)")
        db.execute("INSERT INTO meta(key,value) VALUES('username_index_ready','0') "
                   "ON CONFLICT(key) DO UPDATE SET value='0'")
        db.commit()
        row = db.execute("SELECT value FROM meta WHERE key='username_index_cursor'").fetchone()
        cursor = int(row[0]) if row else 0
        headers_by_id = {}
        for sid, header_json in db.execute("SELECT id, columns_json FROM sources"):
            headers_by_id[sid] = _username_position(json.loads(header_json))
        while True:
            rows = db.execute(
                "SELECT rowid,source_id,fields_json FROM records "
                "WHERE rowid > ? ORDER BY rowid LIMIT 10000", (cursor,)
            ).fetchall()
            if not rows:
                break
            matches = []
            for rowid, source_id, fields in rows:
                pos = headers_by_id.get(source_id)
                if pos is None:
                    continue
                values = json.loads(fields)
                if pos >= len(values):
                    continue
                username = str(values[pos]).strip().lstrip("@").casefold()
                if username and len(username) <= 64 and not any(ch.isspace() for ch in username):
                    matches.append((rowid, username))
            cursor = rows[-1][0]
            with db:
                db.executemany("INSERT OR REPLACE INTO usernames(record_rowid,username_norm) VALUES (?,?)", matches)
                db.execute("INSERT INTO meta(key,value) VALUES('username_index_cursor',?) "
                           "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(cursor),))
            if progress:
                progress(cursor)
        db.execute("CREATE INDEX IF NOT EXISTS idx_usernames_norm ON usernames(username_norm)")
        db.execute("INSERT INTO meta(key,value) VALUES('username_index_ready','1') "
                   "ON CONFLICT(key) DO UPDATE SET value='1'")
        db.commit()
    return username_index_status(path)


def lookup_username(path, username, page=1, page_size=3):
    username = username.strip().lstrip("@").casefold()
    if not username or len(username) > 64 or any(ch.isspace() for ch in username):
        raise ValueError("Provide a valid username")
    if not 1 <= page <= 10000:
        raise ValueError("Invalid page number")
    with connect(path, readonly=True) as db:
        indexed = db.execute("SELECT value FROM meta WHERE key='username_index_ready'").fetchone()
        if not indexed or indexed[0] != '1':
            raise ValueError("Username index not ready. Use /usernameindex")
        rows = db.execute(
            "SELECT s.filename,s.columns_json,r.fields_json,r.user_id "
            "FROM usernames u JOIN records r ON r.rowid=u.record_rowid "
            "JOIN sources s ON s.id=r.source_id "
            "WHERE u.username_norm=? ORDER BY u.record_rowid LIMIT ? OFFSET ?",
            (username,page_size+1,(page-1)*page_size)
        ).fetchall()
    results = []
    for filename, headers_json, fields_json, uid in rows[:page_size]:
        columns = json.loads(headers_json)
        values = json.loads(fields_json)
        pairs = [(name if str(name).strip() else f"column_{i+1}", value)
                 for i,(name,value) in enumerate(zip(columns,values)) if str(value).strip()]
        pairs.extend((f"extra_{i+1}",value) for i,value in enumerate(values[len(columns):]) if str(value).strip())
        results.append((filename, uid, pairs))
    return results, len(rows)>page_size
