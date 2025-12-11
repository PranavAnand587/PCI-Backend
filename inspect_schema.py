# import sqlite3
# import pandas as pd

# DB_PATH = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"

# conn = sqlite3.connect(DB_PATH)

# df_c = pd.read_sql_query("""
#     SELECT 
#         'against' AS table_name,
#         c_aff_resolved AS affiliation
#     FROM against
#     UNION ALL
#     SELECT
#         'by' AS table_name,
#         c_aff_resolved AS affiliation
#     FROM by
# """, conn)

# df_a = pd.read_sql_query("""
#     SELECT 
#         'against' AS table_name,
#         a_aff_resolved AS affiliation
#     FROM against
#     UNION ALL
#     SELECT
#         'by' AS table_name,
#         a_aff_resolved AS affiliation
#     FROM by
# """, conn)

# df = pd.concat([
#     df_c.assign(type="Complainant"),
#     df_a.assign(type="Accused")
# ], ignore_index=True)

# df.to_csv("affiliations_dump.csv", index=False, encoding="utf-8")

# print("Saved affiliations_dump.csv")

# import sqlite3, csv, re
# from pathlib import Path
# DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
# MAPPING = Path(r"d:\Projects\mphasis\pci_project_all\api_dev\affiliation_mappings.csv")

# def norm(s):
#     if s is None: return ""
#     s = str(s).strip()
#     s = re.sub(r"\s+"," ", s)
#     s = s.lower()
#     s = re.sub(r"[^\w\s]","", s)
#     return s

# # load mapping keys
# map_keys = set()
# with MAPPING.open(encoding="utf-8", newline="") as f:
#     reader = csv.DictReader(f)
#     for r in reader:
#         k = (r.get("normalized_key") or r.get("normalized") or "").strip()
#         if k: map_keys.add(k)

# conn = sqlite3.connect(DB)
# cur = conn.cursor()
# tables = ["against","by"]
# db_keys = set()
# for t in tables:
#     cur.execute(f"SELECT DISTINCT c_aff_resolved FROM {t}")
#     db_keys.update([norm(r[0]) for r in cur.fetchall()])
#     cur.execute(f"SELECT DISTINCT a_aff_resolved FROM {t}")
#     db_keys.update([norm(r[0]) for r in cur.fetchall()])

# missing = sorted(k for k in db_keys if k and k not in map_keys)
# print("Missing normalized keys (examples):", missing[:200])
# print("Count missing:", len(missing))
# conn.close()

#!/usr/bin/env python3
"""
Fix case/variant noise for a few high-frequency affiliation values:
- canonicalize editor/editor. -> "Editor"
- canonicalize police authorities / police -> "Police Authorities"

This is conservative and only touches the two columns:
  c_aff_resolved, a_aff_resolved
in both tables: 'against' and 'by'

Backups (c_aff_resolved_backup / a_aff_resolved_backup) are created if absent,
and original values are copied into them (only when backup is empty) to be safe.
"""
import sqlite3
import re

DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
TABLES = ["against", "by"]
COLS = ["c_aff_resolved", "a_aff_resolved"]

# groups of normalized variants -> canonical label
CANONICAL_GROUPS = {
    "Editor": [
        "editor",
        "the editor",
        "editor.",
        "the editor.",
        "editor ",
        "editor,",
        "theeditor",
    ],
    "Police Authorities": [
        "police authorities",
        "police authority",
        "police authorities.",
        "police",
        "police.",
        "police admin",
    ],
}

def norm_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s)
    return s

def ensure_backup_column(cursor, table, col):
    backup_col = f"{col}_backup"
    cursor.execute(f"PRAGMA table_info({table});")
    cols = [c[1] for c in cursor.fetchall()]
    if backup_col not in cols:
        cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{backup_col}" TEXT;')
        print(f"Added backup column {backup_col} to {table}")
    # copy original into backup only where backup is NULL or empty
    cursor.execute(f'''
        UPDATE "{table}"
        SET "{backup_col}" = "{col}"
        WHERE ("{backup_col}" IS NULL OR TRIM("{backup_col}") = '')
    ''')
    return backup_col

def update_canonical_for_group(conn, table, col, normalized_variants, canonical_label):
    cur = conn.cursor()
    # Build placeholders and values for WHERE lower(trim(...)) IN (...)
    # We'll use a normalized check using lower(trim(...)) after removing punctuation in SQL is messy,
    # so fetch candidate rowids and evaluate in Python — safer and portable.
    cur.execute(f'SELECT rowid, "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL AND TRIM("{col}") != ""')
    rows = cur.fetchall()
    to_update = []
    for rowid, val in rows:
        key = norm_key(val)
        if key in normalized_variants:
            to_update.append((canonical_label, rowid))

    if not to_update:
        return 0

    update_stmt = f'UPDATE "{table}" SET "{col}" = ? WHERE rowid = ?'
    cur.executemany(update_stmt, to_update)
    conn.commit()
    return len(to_update)

def show_distinct(conn, table, col):
    cur = conn.cursor()
    cur.execute(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL AND TRIM("{col}") != "" ORDER BY "{col}" COLLATE NOCASE LIMIT 200')
    return [r[0] for r in cur.fetchall()]

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Build normalized sets for quick membership tests
    normalized_map = {}
    for canon, variants in CANONICAL_GROUPS.items():
        normalized_map[canon] = set([norm_key(v) for v in variants])

    total_changed = 0
    for t in TABLES:
        # ensure backups
        for col in COLS:
            ensure_backup_column(cur, t, col)
        conn.commit()

        print(f"\nProcessing table: {t}")
        for canon_label, norm_set in normalized_map.items():
            for col in COLS:
                changed = update_canonical_for_group(conn, t, col, norm_set, canon_label)
                if changed:
                    print(f'  Updated {changed} rows in {t}.{col} -> "{canon_label}"')
                total_changed += changed

        # show a few distinct examples for quick verification
        for col in COLS:
            vals = show_distinct(conn, t, col)
            print(f'  Distinct values for {t}.{col} (sample up to 20):')
            for v in vals[:20]:
                print(f'    - {v}')
    conn.close()
    print(f"\nDone. Total updated rows across tables/cols: {total_changed}")

if __name__ == "__main__":
    main()
