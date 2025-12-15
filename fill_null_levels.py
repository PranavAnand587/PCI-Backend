import sqlite3
import pandas as pd

DB_PATH = 'complaints.db'

def fill_null_levels():
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check current nulls
    cursor.execute('SELECT count(*) FROM "by" WHERE level IS NULL')
    initial_nulls = cursor.fetchone()[0]
    print(f"Initial rows with NULL level: {initial_nulls}")

    if initial_nulls > 0:
        print("Updating NULL levels to 'unknown'...")
        cursor.execute('UPDATE "by" SET level = "unknown" WHERE level IS NULL')
        conn.commit()
        print(f"Updated {cursor.rowcount} rows.")
    else:
        print("No NULL levels found to update.")

    # Verify
    cursor.execute('SELECT count(*) FROM "by" WHERE level IS NULL')
    final_nulls = cursor.fetchone()[0]
    print(f"Final rows with NULL level: {final_nulls}")
    
    # Check distribution
    print("\nLevel distribution:")
    df_dist = pd.read_sql_query('SELECT level, count(*) as count FROM "by" GROUP BY level', conn)
    print(df_dist)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    fill_null_levels()
