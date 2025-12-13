import sqlite3
import sys
import codecs
from collections import defaultdict
import re

sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

DB = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"
conn = sqlite3.connect(DB)
cursor = conn.cursor()

print("="*80)
print("FINDING PLURAL/ARTICLE VARIATIONS IN AFFILIATION COLUMNS")
print("="*80)

def get_base_word(s):
    """Remove 'the', 's' plural, etc."""
    if not s:
        return ""
    s = s.strip().lower()
    # Remove leading "the "
    s = re.sub(r'^the\s+', '', s)
    # Remove trailing 's' for plurals (simple heuristic)
    s = re.sub(r's$', '', s)
    return s

for table in ['against', 'by']:
    print(f"\n{'='*80}")
    print(f"TABLE: {table}")
    print('='*80)
    
    # Check both complainant and accused affiliations
    for col_name in ['c_aff_resolved', 'a_aff_resolved']:
        print(f"\n--- Column: {col_name} ---")
        
        cursor.execute(f"SELECT DISTINCT {col_name} FROM {table} WHERE {col_name} IS NOT NULL")
        all_values = [row[0] for row in cursor.fetchall()]
        
        # Group by base word
        groups = defaultdict(list)
        for val in all_values:
            base = get_base_word(val)
            if base:
                groups[base].append(val)
        
        # Find groups with multiple variations
        variations = {k: v for k, v in groups.items() if len(v) > 1}
        
        # Get counts for each variation
        variation_counts = []
        for base, vals in variations.items():
            total = 0
            val_counts = []
            for val in vals:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col_name} = ?", (val,))
                cnt = cursor.fetchone()[0]
                total += cnt
                val_counts.append((val, cnt))
            variation_counts.append((base, val_counts, total))
        
        # Sort by total count
        variation_counts.sort(key=lambda x: -x[2])
        
        print(f"Found {len(variations)} groups with variations")
        print(f"\nTop 20 groups (showing variations):")
        
        for base, val_counts, total in variation_counts[:20]:
            print(f"\n  Base: '{base}' (Total: {total})")
            for val, cnt in sorted(val_counts, key=lambda x: -x[1]):
                print(f"    [{cnt:4}] {repr(val)}")

conn.close()
