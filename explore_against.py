import sqlite3
import sys
import codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
conn = sqlite3.connect(DB)
cursor = conn.cursor()

print("="*80)
print("ANALYZING 'Against' COLUMN FOR NORMALIZATION")
print("="*80)

for table in ['against', 'by']:
    print(f"\n--- TABLE: {table} ---")
    
    # Get total unique values
    cursor.execute(f"SELECT COUNT(DISTINCT Against) FROM {table}")
    unique_count = cursor.fetchone()[0]
    print(f"Total unique 'Against' values: {unique_count}")
    
    # Get top 30 most common values
    cursor.execute(f"""
        SELECT Against, COUNT(*) as cnt 
        FROM {table} 
        GROUP BY Against 
        ORDER BY cnt DESC 
        LIMIT 30
    """)
    results = cursor.fetchall()
    print(f"\nTop 30 most common values:")
    for val, cnt in results:
        print(f"  [{cnt:4}] {repr(val)}")
    
    # Look for potential duplicates (case variations, punctuation, etc.)
    print(f"\n--- Potential duplicates (case/punctuation variations) ---")
    cursor.execute(f"SELECT DISTINCT Against FROM {table} ORDER BY Against")
    all_values = [row[0] for row in cursor.fetchall() if row[0]]
    
    # Group by normalized form
    from collections import defaultdict
    import re
    
    def normalize_key(s):
        if not s:
            return ""
        s = s.lower().strip()
        s = re.sub(r'\s+', ' ', s)
        s = re.sub(r'[^\w\s]', '', s)
        return s
    
    groups = defaultdict(list)
    for val in all_values:
        key = normalize_key(val)
        groups[key].append(val)
    
    # Find groups with multiple variations
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"Found {len(duplicates)} groups with variations")
    
    # Show top 20 by total count
    duplicate_counts = []
    for key, variations in duplicates.items():
        total = 0
        for var in variations:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE Against = ?", (var,))
            total += cursor.fetchone()[0]
        duplicate_counts.append((key, variations, total))
    
    duplicate_counts.sort(key=lambda x: -x[2])
    
    print("\nTop 20 groups with variations (by total count):")
    for key, variations, total in duplicate_counts[:20]:
        print(f"\n  Total: {total} | Normalized: '{key}'")
        for var in variations:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE Against = ?", (var,))
            cnt = cursor.fetchone()[0]
            print(f"    [{cnt:3}] {repr(var)}")

conn.close()
