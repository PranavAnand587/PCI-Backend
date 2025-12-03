import sqlite3

db_path = r'd:\Projects\mphasis\pci_project_all\api_dev\complaints.db'

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()

    for tbl in ('against', 'by'):
        print(f'\n=== {tbl.upper()} TABLE ===')
        cur.execute(f'SELECT DISTINCT Compliant_Aff FROM {tbl} ORDER BY ComplaintType')
        print('ComplaintType :', [row[0] for row in cur.fetchall()])

        # cur.execute(f'SELECT DISTINCT res_ComplaintType FROM {tbl} ORDER BY res_ComplaintType')
        # print('res_ComplaintType :', [row[0] for row in cur.fetchall()])