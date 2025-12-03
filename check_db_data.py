import sqlite3

db_path = r'd:\Projects\mphasis\pci_project_all\api_dev\complaints.db'

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()

    for tbl in ('against', 'by'):
        print(f'\n=== {tbl.upper()} TABLE ===')
        cur.execute(f'SELECT COUNT(*) FROM {tbl}')
        count = cur.fetchone()[0]
        print(f'Total rows: {count}')
        
        # Sample a few rows
        cur.execute(f'SELECT Complainant, Against, State, ComplaintType, Decision FROM {tbl} LIMIT 5')
        print('\nSample rows:')
        for row in cur.fetchall():
            print(f'  Complainant: {row[0]}, Against: {row[1]}, State: {row[2]}, Type: {row[3]}, Decision: {row[4]}')
