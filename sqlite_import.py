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
    if not source_root.is_dir() or source_root.parent != root:
        raise ValueError("No extracted job folder found in DATA_DIR")
    database = db_path(root)
    database.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(database.parent, 0o700)
    paths = sorted(p for p in source_root.rglob("*") if p.is_file() and p.suffix.lower() == ".csv")
    if not paths:
        raise ValueError("No CSV files were found")
    with connect(database) as db:
        create_schema(db)
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
