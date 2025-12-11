import sqlite3
import csv
import re
from pathlib import Path

DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
TABLES = ["against", "by"]
COLS = {
    "c": "c_aff_resolved",
    "a": "a_aff_resolved"
}
MAPPING_CSV = Path(r"d:\Projects\mphasis\pci_project_all\api_dev\affiliation_mappings.csv")

def simple_norm(s):
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s)  # remove punctuation
    return s

def load_mapping(csv_path):
    m = {}
    if not csv_path.exists():
        print("Mapping CSV not found:", csv_path)
        return m
    with csv_path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # expect normalized_key, suggested_canonical
        for r in reader:
            key = r.get("normalized_key") or r.get("normalized")
            canon = r.get("suggested_canonical") or r.get("canonical")
            if key is None or canon is None:
                continue
            key_norm = str(key).strip()
            if key_norm:
                m[key_norm] = canon.strip()
    return m

def ensure_backup_column(cursor, table, col):
    # Make a backup column named <col>_backup if not exists
    backup_col = f"{col}_backup"
    cursor.execute(f"PRAGMA table_info({table});")
    cols = [c[1] for c in cursor.fetchall()]
    if backup_col not in cols:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN \"{backup_col}\" TEXT;")
        print(f"Added backup column {backup_col} to {table}")
    
    # Copy data (only where backup is empty) to avoid overwriting previous backups
    # This ensures if we run multiple times, we keep the ORIGINAL original
    cursor.execute(f"""
        UPDATE {table}
        SET "{backup_col}" = "{col}"
        WHERE "{backup_col}" IS NULL
    """)
    return backup_col

def canonicalize_value(raw, mapping):
    if raw is None:
        return None
    raw_str = str(raw).strip()
    if raw_str == "":
        return raw_str  # leave blank entries blank
    
    # 1. Try exact mapping match
    key = simple_norm(raw_str)
    if key in mapping:
        return mapping[key]
    
    # 2. Fallback: General cleanup
    # Remove stray punctuation
    cleaned = re.sub(r"[^\w\s]", "", raw_str)
    # Collapse spaces
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Title case first few words (heuristic)
    parts = cleaned.split()
    # Capitalize words, but keep small words small if not first? 
    # For simplicity, just capitalize all for now, or use title()
    # But title() messes up 'BJP' -> 'Bjp'.
    # Let's just return the cleaned string if no mapping found, maybe just Capitalize first letter
    if not parts:
        return ""
    
    # Simple Title Case for fallback
    return cleaned.title()

def update_table(conn, table, mapping):
    cur = conn.cursor()
    
    # Ensure backups exist and are filled
    for short, col in COLS.items():
        ensure_backup_column(cur, table, col)
    conn.commit()

    # Select PK for iteration - try PrimaryKey, else rowid
    cur.execute(f"PRAGMA table_info({table});")
    schema = cur.fetchall()
    cols = [c[1] for c in schema]
    # We don't have a formal PK in the schema usually, so use rowid
    pk = "rowid"

    # Fetch all rows with the two columns and PK
    cur.execute(f"SELECT {pk}, {COLS['c']}, {COLS['a']} FROM {table}")
    rows = cur.fetchall()
    total_rows = len(rows)
    print(f"Processing {table}: {total_rows} rows")

    update_stmt = f"UPDATE {table} SET \"{COLS['c']}\" = ?, \"{COLS['a']}\" = ? WHERE {pk} = ?"
    changed = 0

    for row in rows:
        rowid = row[0]
        raw_c = row[1]
        raw_a = row[2]
        
        new_c = canonicalize_value(raw_c, mapping) if raw_c is not None else raw_c
        new_a = canonicalize_value(raw_a, mapping) if raw_a is not None else raw_a

        # Only run update if something actually changes (string compare)
        # We check against the RAW values we just fetched
        if (raw_c != new_c) or (raw_a != new_a):
            cur.execute(update_stmt, (new_c, new_a, rowid))
            changed += 1

    conn.commit()
    print(f"Updated {changed} rows in {table} (out of {total_rows})")
    return changed, total_rows

def main():
    mapping = load_mapping(MAPPING_CSV)
    print(f"Loaded mapping entries: {len(mapping)}")

    conn = sqlite3.connect(DB)
    total_changed = 0
    total_rows = 0
    for t in TABLES:
        changed, rows = update_table(conn, t, mapping)
        total_changed += changed
        total_rows += rows
    conn.close()
    print(f"\nDone. Total updated rows across tables: {total_changed} / {total_rows}")

if __name__ == "__main__":
    main()
