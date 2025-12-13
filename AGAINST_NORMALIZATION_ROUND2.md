# Against Column - Additional Normalization (Round 2)

Successfully applied additional normalization rules to consolidate plural and article variations.

## Summary

### Against Table
- **Previous**: 938 unique values
- **Now**: 910 unique values
- **Additional reduction**: 28 values (6.2% total reduction from original 970)
- **Rows updated this round**: 129

### By Table
- **Previous**: 760 unique values  
- **Now**: 745 unique values
- **Additional reduction**: 15 values (7.1% total reduction from original 802)
- **Rows updated this round**: 23

**Total this round**: 152 rows updated

---

## Key Consolidations

### "Editor" Variations → "Editor"
- `The Editor` (61 instances) → `Editor`
- `the editor` (29 instances) → `Editor`  
- `Editors` (9 instances) → `Editor`
- **Total now**: 2,501 entries under "Editor"

### Publication Name Variations
- `The Indian Express` (15) → `Indian Express` (now 62 total)
- `The Eenadu` (4) → `Eenadu`
- `Samaj` (7) → `The Samaj` (now 16 total)
- `Statesman` (3) → `The Statesman` (now 22 total)
- `Hindu` (1) → `The Hindu` (now 15 total)
- `Asian Age` (2) → `The Asian Age` (now 15 total)
- `The Deccan Chronicle` (1) → `Deccan Chronicle` (now 17 total)
- `Economic Times` (2) → `The Economic Times` (now 11 total)
- `The Punjab Kesari` (1) → `Punjab Kesari` (now 13 total)
- `The Maharashtra Times` (1) → `Maharashtra Times` (now 11 total)

### Authority Variations (By Table)
- `The Director` (4) → `Director` (now 55 total)
- `journalists` (2) → `Journalist`
- `The Station House Officer` (1) → `Station House Officer`
- `The Executive Engineer` (2) → `Executive Engineer`
- `The Collector` (1) → `Collector`
- `The President` (1) → `President`

---

## Mapping Rules Added

Added **60 new mapping rules** to handle:
1. **Article variations**: "the X" → canonical form
2. **Plural variations**: "Editors" → "Editor", "journalists" → "Journalist"
3. **Case variations**: Already handled in previous round

**Total mapping rules**: 127 (up from 67)

---

## Impact

The **"Top Targeted News Sources"** chart will now show:
- **"Editor"** as a single consolidated entry (2,501 complaints)
- Cleaner grouping of publication names without "the" variations
- More accurate representation of complaint distribution

---

## Files Updated

- **[against_mappings.csv](file:///d:/Projects/mphasis/pci_project_all/api_dev/against_mappings.csv)** - Now contains 127 mapping rules
- Database columns `Against` in both tables updated
- Original data preserved in `Against_backup` columns
