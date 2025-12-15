import sqlite3

DB_PATH = r"d:\Projects\mphasis\pci_project_all\api_dev\complaints copy.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Inspecting database schema...")

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_name_tuple in tables:
        table_name = table_name_tuple[0]
        print(f"\nTable: {table_name}")
        print("--------------------")

        # Get column information for each table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        for col in columns:
            cid, name, ctype, notnull, dflt_value, pk = col
            print(f"  Column ID: {cid}, Name: {name}, Type: {ctype}, Not Null: {bool(notnull)}, Default Value: {dflt_value}, Primary Key: {bool(pk)}")

    conn.close()
    print("\nSchema inspection complete.")

if __name__ == "__main__":
    main()
