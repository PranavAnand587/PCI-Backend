import sqlite3
import sys
import codecs
import re

sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

conn = sqlite3.connect('complaints.db')
cursor = conn.cursor()

def normalize_for_comparison(s):
    """Normalize a string for comparison purposes"""
    if s is None:
        return ""
    s = s.lower().strip()
    s = re.sub(r'\s+', ' ', s)  # Multiple spaces to single
    s = s.rstrip('.')  # Remove trailing period
    s = s.replace('–', '-')  # En-dash to hyphen
    return s

# Find all near-duplicates
print("="*80)
print("FINDING NEAR-DUPLICATE VALUES (differ only by case, spaces, punctuation)")
print("="*80)

for table in ['against', 'by']:
    cursor.execute(f"SELECT Decision_Specific, COUNT(*) FROM {table} GROUP BY Decision_Specific")
    results = cursor.fetchall()
    
    # Group by normalized form
    normalized_groups = {}
    for val, cnt in results:
        if val is None:
            continue
        norm = normalize_for_comparison(val)
        if norm not in normalized_groups:
            normalized_groups[norm] = []
        normalized_groups[norm].append((val, cnt))
    
    # Find groups with multiple variations
    duplicates = {k: v for k, v in normalized_groups.items() if len(v) > 1}
    
    print(f"\n--- {table} table: {len(duplicates)} groups with near-duplicates ---")
    for norm, variations in sorted(duplicates.items(), key=lambda x: -sum(v[1] for v in x[1]))[:30]:
        total = sum(v[1] for v in variations)
        print(f"\n  Normalized: '{norm}' (Total: {total})")
        for val, cnt in sorted(variations, key=lambda x: -x[1]):
            print(f"    [{cnt}] {repr(val)}")

# Also show what the canonical form would be for each group
print("\n" + "="*80)
print("SUGGESTED CANONICAL FORMS")
print("="*80)

def get_canonical(variations):
    """Pick the best canonical form from variations"""
    # Prefer: no trailing period, title case, no extra spaces
    best = None
    best_score = -1
    for val, cnt in variations:
        score = cnt  # Start with count as base score
        if not val.endswith('.'):
            score += 100
        if val == val.strip():
            score += 50
        if '  ' not in val:
            score += 25
        if score > best_score:
            best_score = score
            best = val
    return best

for table in ['against', 'by']:
    cursor.execute(f"SELECT Decision_Specific, COUNT(*) FROM {table} GROUP BY Decision_Specific")
    results = cursor.fetchall()
    
    normalized_groups = {}
    for val, cnt in results:
        if val is None:
            continue
        norm = normalize_for_comparison(val)
        if norm not in normalized_groups:
            normalized_groups[norm] = []
        normalized_groups[norm].append((val, cnt))
    
    duplicates = {k: v for k, v in normalized_groups.items() if len(v) > 1}
    
    print(f"\n--- {table} table ---")
    for norm, variations in sorted(duplicates.items(), key=lambda x: -sum(v[1] for v in x[1]))[:15]:
        total = sum(v[1] for v in variations)
        canonical = get_canonical(variations)
        if canonical:
            # Apply additional cleanup to canonical
            canonical_clean = canonical.strip()
            canonical_clean = re.sub(r'\s+', ' ', canonical_clean)
            if canonical_clean[0].islower():
                canonical_clean = canonical_clean[0].upper() + canonical_clean[1:]
            canonical_clean = canonical_clean.rstrip('.')
            print(f"  {repr(norm)[:40]:40} -> {repr(canonical_clean)}")

conn.close()
