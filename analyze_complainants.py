import sqlite3
import pandas as pd

DB_PATH = 'complaints.db'

def analyze_complainants():
    conn = sqlite3.connect(DB_PATH)
    print("Top 50 Complainants in 'by' table:")
    df = pd.read_sql_query('SELECT Complainant, count(*) as c FROM "by" GROUP BY Complainant ORDER BY c DESC LIMIT 50', conn)
    print(df)
    conn.close()

if __name__ == "__main__":
    analyze_complainants()
