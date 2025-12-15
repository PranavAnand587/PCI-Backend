import sqlite3
import pandas as pd

DB_PATH = 'complaints.db'

# List of generic titles to normalize
GENERIC_TITLES = [
    "Editor", "Chief Editor", "Publisher", "President", "General Secretary", 
    "Manager", "editor", "Correspondent", "Reporter", "Journalist", "Secretary",
    "Managing Editor", "Group Editor", "Resident Editor", "Executive Editor",
    "Bureau Chief", "Staff Reporter", "City Editor", "News Editor", "Sub-Editor"
]

def normalize_complainants():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Normalizing generic Complainant names in 'by' table...")
    
    updates = 0
    
    # Fetch all rows where Complainant is in our generic list
    # We'll do this in python to be case-insensitive or flexible
    
    cursor.execute('SELECT PrimaryKey, Complainant, Complainant_Aff FROM "by"')
    rows = cursor.fetchall()
    
    batch_updates = []
    
    for row in rows:
        pk, comp, aff = row
        
        if not comp:
            continue
            
        # Check if comp is in our generic list (case-insensitive check)
        if comp.strip() in GENERIC_TITLES or comp.strip().lower() in [t.lower() for t in GENERIC_TITLES]:
            if aff and aff.strip():
                new_name = f"{comp.strip()}, {aff.strip()}"
                batch_updates.append((new_name, pk))
                # print(f"Updating: '{comp}' -> '{new_name}'")
    
    if batch_updates:
        print(f"Found {len(batch_updates)} rows to update.")
        cursor.executemany('UPDATE "by" SET Complainant = ? WHERE PrimaryKey = ?', batch_updates)
        conn.commit()
        print(f"Successfully updated {cursor.rowcount} rows.")
    else:
        print("No rows found requiring update.")
        
    conn.close()

if __name__ == "__main__":
    normalize_complainants()
