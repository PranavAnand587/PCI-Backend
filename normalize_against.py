import sqlite3
import csv
import re
from pathlib import Path

DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
TABLES = ["against", "by"]
COLUMN = "Against"
MAPPING_CSV = Path(r"d:\Projects\mphasis\pci_project_all\api_dev\against_mappings.csv")

def simple_norm(s):
    """Normalize string for matching purposes"""
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    s = re.sub(r"[^\w\s]", "", s)  # remove punctuation
    return s

def load_mapping(csv_path):
    """Load normalization mappings from CSV"""
    m = {}
    if not csv_path.exists():
        print(f"Mapping CSV not found: {csv_path}")
        return m
    with csv_path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
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
    """Create backup column if it doesn't exist"""
    backup_col = f"{col}_backup"
    cursor.execute(f"PRAGMA table_info({table});")
    cols = [c[1] for c in cursor.fetchall()]
    if backup_col not in cols:
        cursor.execute(f'ALTER TABLE {table} ADD COLUMN "{backup_col}" TEXT;')
        print(f"Added backup column {backup_col} to {table}")
    
    # Copy data (only where backup is empty) to preserve original
    cursor.execute(f"""
        UPDATE {table}
        SET "{backup_col}" = "{col}"
        WHERE "{backup_col}" IS NULL
    """)
    return backup_col

def canonicalize_value(raw, mapping):
    """Canonicalize a value using the mapping, with fallback cleanup"""
    if raw is None:
        return None
    raw_str = str(raw).strip()
    if raw_str == "":
        return raw_str
    
    # 1. Try exact mapping match
    key = simple_norm(raw_str)
    if key in mapping:
        return mapping[key]
    
    # 2. Fallback: General cleanup
    # Remove hyphens that split words (e.g., "Edi-tor" -> "Editor")
    cleaned = re.sub(r'(\w)-(\w)', r'\1\2', raw_str)
    # Remove other punctuation
    cleaned = re.sub(r"[^\w\s&]", "", cleaned)
    # Collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    
    # If no mapping found, return cleaned version
    if not cleaned:
        return ""
    
    return cleaned

def update_table(conn, table, mapping):
    """Update the Against column in a table"""
    cur = conn.cursor()
    
    # Ensure backup exists
    ensure_backup_column(cur, table, COLUMN)
    conn.commit()

    # Use rowid as primary key
    pk = "rowid"

    # Fetch all rows
    cur.execute(f'SELECT {pk}, "{COLUMN}" FROM {table}')
    rows = cur.fetchall()
    total_rows = len(rows)
    print(f"Processing {table}: {total_rows} rows")

    update_stmt = f'UPDATE {table} SET "{COLUMN}" = ? WHERE {pk} = ?'
    changed = 0

    for row in rows:
        rowid = row[0]
        raw_value = row[1]
        
        new_value = canonicalize_value(raw_value, mapping) if raw_value is not None else raw_value

        # Only update if something changed
        if raw_value != new_value:
            cur.execute(update_stmt, (new_value, rowid))
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
