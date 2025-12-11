import sqlite3
conn = sqlite3.connect(r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db")
cursor = conn.cursor()

print("--- Verification ---")

# Check Backups
cursor.execute("SELECT COUNT(*) FROM against WHERE c_aff_resolved_backup IS NOT NULL")
print(f"Rows with backups (Against): {cursor.fetchone()[0]}")

# Check Normalization Effectiveness
cursor.execute("SELECT COUNT(DISTINCT c_aff_resolved) FROM against")
print(f"Unique Complainant Affiliations (Against) After: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(DISTINCT a_aff_resolved) FROM against")
print(f"Unique Accused Affiliations (Against) After: {cursor.fetchone()[0]}")

# Check Specific Examples
print("\n--- Times of India Variations ---")
cursor.execute("SELECT DISTINCT a_aff_resolved FROM against WHERE a_aff_resolved LIKE '%Times of India%'")
for row in cursor.fetchall():
    print(row[0])

print("\n--- Govt of UP Variations ---")
cursor.execute("SELECT DISTINCT c_aff_resolved FROM against WHERE c_aff_resolved LIKE '%Govt%UP%' OR c_aff_resolved LIKE '%Uttar Pradesh%'")
for row in cursor.fetchall():
    print(row[0])

conn.close()
