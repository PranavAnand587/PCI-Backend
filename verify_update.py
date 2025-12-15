import sqlite3
import pandas as pd

DB_PATH = 'complaints.db'

def verify():
    conn = sqlite3.connect(DB_PATH)
    print("Verifying 'by' table update...")
    
    # Check schema
    cursor = conn.cursor()
    cursor.execute('PRAGMA table_info("by")')
    cols = [info[1] for info in cursor.fetchall()]
    if 'level' in cols:
        print("'level' column exists.")
    else:
        print("'level' column MISSING.")
        return

    # Check data
    df = pd.read_sql_query('SELECT Complainant, level FROM "by" WHERE level IS NOT NULL LIMIT 10', conn)
    print("\nSample Data:")
    print(df)
    
    # Check nulls
    null_count = pd.read_sql_query('SELECT count(*) FROM "by" WHERE level IS NULL', conn).iloc[0,0]
    print(f"\nRows with NULL level: {null_count}")
    
    conn.close()

if __name__ == "__main__":
    verify()
