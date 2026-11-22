"""Aggregate-only CSV schema and quality checks; never print or store records."""
import csv
import json
from collections import Counter
from pathlib import Path

ID_NAMES = {"telegram id", "user id", "user_id", "account_id", "id"}
DELIMITERS = (",", ";", "|", "\t")


def audit_folder(folder, sample_limit=1000):
    folder = Path(folder)
    reports = []
    for path in sorted(folder.rglob("*")):
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue
        result = {
            "file": path.relative_to(folder).as_posix(),
            "bytes": path.stat().st_size,
            "sampled": 0,
            "numeric_id": 0,
            "missing_id": 0,
            "malformed_rows": 0,
            "column_counts": {},
        }
        try:
            with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as stream:
                first = stream.readline()
                delimiter = max(DELIMITERS, key=first.count)
                result["delimiter"] = repr(delimiter)
                stream.seek(0)
                rows = csv.reader(stream, delimiter=delimiter)
                header = next(rows)
                index = next(
                    (i for i, value in enumerate(header)
                     if value.strip().lower() in ID_NAMES), None
                )
                result["id_column"] = index
                result["header_fields"] = len(header)
                distribution = Counter()
                for row in rows:
                    result["sampled"] += 1
                    distribution[len(row)] += 1
                    if len(row) != len(header):
                        result["malformed_rows"] += 1
                    if index is None or index >= len(row):
                        result["missing_id"] += 1
                    else:
                        value = row[index].strip()
                        if value.isascii() and value.isdecimal():
                            result["numeric_id"] += 1
                        else:
                            result["missing_id"] += 1
                    if result["sampled"] >= sample_limit:
                        break
                result["column_counts"] = {str(k): v for k, v in sorted(distribution.items())}
        except Exception as exc:
            result["error"] = type(exc).__name__
        reports.append(result)
    return {
        "sample_limit": sample_limit,
        "files": reports,
        "csv_count": len(reports),
        "numeric_id_samples": sum(f["numeric_id"] for f in reports),
        "total_samples": sum(f["sampled"] for f in reports),
        "files_with_id_column": sum(f.get("id_column") is not None for f in reports if "error" not in f),
        "files_with_malformed_samples": sum(bool(f["malformed_rows"]) for f in reports),
        "errors": sum("error" in f for f in reports),
    }


def write_audit(folder, report):
    dest = Path(folder) / "_csv_audit.json"
    temp = dest.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8") as out:
        json.dump(report, out, ensure_ascii=False, indent=2)
    temp.replace(dest)
    return dest


def summary(report):
    return (
        f"📊 CSV audit: {report['csv_count']} CSV files\n"
        f"ID columns found: {report['files_with_id_column']}/{report['csv_count']}\n"
        f"Numeric IDs (sample): {report['numeric_id_samples']}/{report['total_samples']}\n"
        f"Files with inconsistent sampled row widths: {report['files_with_malformed_samples']}\n"
        f"Files with errors: {report['errors']}\n"
        f"Sample: up to {report['sample_limit']} rows/file.\n"
        "This is a sample-based quality check, not a full import."
    )
