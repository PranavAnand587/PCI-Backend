import pandas as pd
import sqlite3

CSV_PATH = 'final_by_press_with_level.csv'
DB_PATH = 'complaints.db'

def update_database():
    print(f"Reading CSV from {CSV_PATH}...")
    try:
        # Using semicolon separator as determined from previous inspection
        df_csv = pd.read_csv(CSV_PATH, sep=';', on_bad_lines='skip', engine='python')
    except Exception as e:
        print(f"Failed to read CSV: {e}")
        return

    if 'Complainant' not in df_csv.columns or 'level' not in df_csv.columns:
        print(f"CSV missing 'Complainant' or 'level' columns. Available: {df_csv.columns.tolist()}")
        return

    # Create mapping: Complainant -> level
    comp_to_level = {}
    
    print("Building Complainant -> Level mapping...")
    for index, row in df_csv.iterrows():
        comp = row['Complainant']
        lvl = row['level']
        
        if pd.isna(comp) or pd.isna(lvl):
            continue
            
        comp = str(comp).strip()
        lvl = str(lvl).strip()
        
        # If we encounter the same complainant with a different level, we keep the first one
        # (or we could try to be smarter, but for now simple mapping)
        if comp not in comp_to_level:
            comp_to_level[comp] = lvl

    print(f"Found {len(comp_to_level)} unique Complainants with levels in CSV.")

    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check if 'level' column exists in 'by' table
    cursor.execute('PRAGMA table_info("by")')
    columns = [info[1] for info in cursor.fetchall()]
    
    if 'level' not in columns:
        print("Adding 'level' column to 'by' table...")
        cursor.execute('ALTER TABLE "by" ADD COLUMN level TEXT')
    else:
        print("'level' column already exists in 'by' table.")

    # Update the database
    print("Updating 'by' table based on Complainant column...")
    
    # Prepare batch update
    update_data = [(lvl, comp) for comp, lvl in comp_to_level.items()]
    
    # We use executemany to update rows where Complainant matches
    cursor.executemany('UPDATE "by" SET level = ? WHERE Complainant = ?', update_data)
    
    conn.commit()
    
    # Verify
    cursor.execute('SELECT count(*) FROM "by" WHERE level IS NOT NULL')
    filled_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT count(*) FROM "by"')
    total_count = cursor.fetchone()[0]
    
    print(f"Total rows in 'by' table: {total_count}")
    print(f"Total rows with 'level' populated: {filled_count}")
    print(f"Coverage: {filled_count/total_count*100:.2f}%")
    
    conn.close()
    print("Done.")

if __name__ == "__main__":
    update_database()
