import sqlite3

DB_PATH = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Normalizing state names...")

    updates = [
        # against table
        ("UPDATE against SET State = 'Odisha' WHERE State IN ('Orissa', 'Orrisa')", "against: Orissa/Orrisa → Odisha"),
        ("UPDATE against SET State = 'Chhattisgarh' WHERE State = 'Chattisgarh'", "against: Chattisgarh → Chhattisgarh"),

        # by table
        ("UPDATE by SET State = 'Odisha' WHERE State IN ('Orissa', 'Orrisa')", "by: Orissa/Orrisa → Odisha"),
        ("UPDATE by SET State = 'Chhattisgarh' WHERE State = 'Chattisgarh'", "by: Chattisgarh → Chhattisgarh"),
    ]

    for sql, label in updates:
        cursor.execute(sql)
        print(f"{label} | rows affected: {cursor.rowcount}")

    conn.commit()
    conn.close()

    print("Done.")

if __name__ == "__main__":
    main()
