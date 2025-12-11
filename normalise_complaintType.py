#!/usr/bin/env python3
"""
Normalize values **in-place** in the existing column `ComplaintType_Normalized`.

- Creates a backup column `ComplaintType_Normalized_backup` (only if missing) and copies the original values there.
- Applies a conservative CANONICAL_MAP to replace variants directly in `ComplaintType_Normalized`.
- Writes a CSV with final distribution for review.

Run: python normalize_complainttypes_inplace.py
"""
import sqlite3
import csv
import os
from collections import Counter

DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
TABLES = ["against", "by"]
COL = "ComplaintType_Normalized"
BACKUP_COL = f"{COL}_backup"
OUT_CSV = "complaint_type_normalized_after_counts.csv"

# Conservative canonical mapping based on your inspection
CANONICAL_MAP = {
    # Harassment singular -> plural
    "Harassment of Newsman": "Harassment of Newsmen",
    # Curtailment variants -> unified label
    "Curtailment of Press Freedom": "Curtailment of Press Freedom",
    "Curtailment of the Press Freedom": "Curtailment of Press Freedom",
    "Curtailment to the Press": "Curtailment of Press Freedom",
    "Curtailment": "Curtailment of Press Freedom",
    # Voilence misspellings -> fix to 'Violence against Newsmen'
    "Voilence against Newsmen": "Violence against Newsmen",
    "Voilence of Newsmen": "Violence against Newsmen",
    # Suo-Motu variants
    "Suo-Motu Cognizance": "Suo-Motu",
    "Suo-Motu": "Suo-Motu",
    # keep already-canonical forms (explicit)
    "Harassment of Newsmen": "Harassment of Newsmen",
    "Paid News": "Paid News",
    "Misleading Advertisements": "Misleading Advertisements",
    "Principles and Defamation": "Principles and Defamation",
    "Principles and Publications": "Principles and Publications",
    "Communal, Casteist, Anti National and Anti Religious Writings": "Communal, Casteist, Anti National and Anti Religious Writings",
}

def ensure_column(cursor, table, col_name):
    cursor.execute(f"PRAGMA table_info({table});")
    cols = [c[1] for c in cursor.fetchall()]
    if col_name not in cols:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} TEXT;")
        return True
    return False

def normalize_value(val):
    if val is None:
        return None
    v = str(val).strip()
    if v == "":
        return None
    return CANONICAL_MAP.get(v, v)

def apply_inplace(conn, table):
    cur = conn.cursor()
    # ensure column exists
    cur.execute(f"PRAGMA table_info({table});")
    cols = [c[1] for c in cur.fetchall()]
    if COL not in cols:
        raise RuntimeError(f"Column {COL} not found in table {table}")

    # create backup column if missing and populate
    if BACKUP_COL not in cols:
        print(f"Adding backup column {BACKUP_COL} to {table} and copying original values.")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {BACKUP_COL} TEXT;")
        cur.execute(f"UPDATE {table} SET {BACKUP_COL} = {COL};")
        conn.commit()
    else:
        print(f"Backup column {BACKUP_COL} already exists in {table} (skipping create).")

    # determine pk for updates: prefer PrimaryKey if present, else use rowid
    pk_col = None
    if "PrimaryKey" in cols:
        pk_col = "PrimaryKey"
    else:
        pk_col = "rowid"

    print(f"Processing table {table} using PK {pk_col}")

    # fetch values to update
    cur.execute(f"SELECT {pk_col}, {COL} FROM {table}")
    rows = cur.fetchall()

    update_stmt = f"UPDATE {table} SET {COL} = ? WHERE {pk_col} = ?"
    counts = Counter()
    updated = 0

    for pk, cur_val in rows:
        source_val = (str(cur_val).strip() if cur_val is not None and str(cur_val).strip() != "" else None)
        if source_val is None:
            new_val = None
        else:
            new_val = normalize_value(source_val)
        # only run update if different (avoids writing identical values)
        if (source_val != new_val):
            cur.execute(update_stmt, (new_val, pk))
            updated += 1
        if new_val:
            counts[new_val] += 1

    conn.commit()
    print(f"Updated {updated} rows in {table} (wrote only when value changed).")
    return counts

def main():
    if not os.path.exists(DB):
        print("DB not found:", DB)
        return

    conn = sqlite3.connect(DB)
    total_counts = Counter()

    for t in TABLES:
        # guard: skip if table doesn't exist
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (t,))
        if cur.fetchone() is None:
            print(f"Table {t} not found, skipping.")
            continue
        counts = apply_inplace(conn, t)
        total_counts.update(counts)

    # write out CSV of canonical counts after normalization
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["canonical_label", "count"])
        for label, cnt in total_counts.most_common():
            writer.writerow([label, cnt])

    print(f"Wrote canonical counts CSV: {OUT_CSV}")
    print("\nTop canonical labels across all tables after normalize:")
    for label, cnt in total_counts.most_common(50):
        print(f"{cnt:6d}  {label}")

    conn.close()

if __name__ == "__main__":
    main()
