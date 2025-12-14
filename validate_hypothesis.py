import sqlite3
import pandas as pd
import os

# Set paths
base_path = r"d:\Projects\mphasis\pci_project_all\api_dev"
db_path = os.path.join(base_path, "complaints.db")

def validate_hypothesis():
    with open("hypothesis_results.txt", "w", encoding="utf-8") as f:
        f.write("="*80 + "\n")
        f.write("VALIDATING HYPOTHESIS: Role Ambiguity in 'by' and 'against' tables\n")
        f.write("="*80 + "\n")
        
        conn = sqlite3.connect(db_path)
        
        # ---------------------------------------------------------
        # 1. Analyze 'by' table (Expected: Complainant = Media)
        # ---------------------------------------------------------
        f.write("\n--- Analyzing 'by' table (Expectation: Complainant should be Media) ---\n")
        
        # Query: How many rows have Media as Complainant vs Accused?
        query_by = """
        SELECT 
            CASE 
                WHEN Complainant_Category = 'Media' THEN 'Media' 
                ELSE 'Non-Media' 
            END as Complainant_Role,
            CASE 
                WHEN Accused_Category = 'Media' THEN 'Media' 
                ELSE 'Non-Media' 
            END as Accused_Role,
            COUNT(*) as Count
        FROM "by"
        GROUP BY Complainant_Role, Accused_Role
        ORDER BY Count DESC
        """
        
        df_by = pd.read_sql_query(query_by, conn)
        f.write(df_by.to_string(index=False) + "\n")
        
        # Calculate "Swapped" candidates in 'by' table
        # Candidate = Complainant is NOT Media, but Accused IS Media
        swapped_by = df_by[
            (df_by['Complainant_Role'] == 'Non-Media') & 
            (df_by['Accused_Role'] == 'Media')
        ]['Count'].sum()
        
        total_by = df_by['Count'].sum()
        f.write(f"\nPotential Swapped Rows in 'by': {swapped_by} / {total_by} ({swapped_by/total_by*100:.1f}%)\n")


        # ---------------------------------------------------------
        # 2. Analyze 'against' table (Expected: Accused = Media)
        # ---------------------------------------------------------
        f.write("\n\n--- Analyzing 'against' table (Expectation: Accused should be Media) ---\n")
        
        # Query: How many rows have Media as Complainant vs Accused?
        query_against = """
        SELECT 
            CASE 
                WHEN Complainant_Category = 'Media' THEN 'Media' 
                ELSE 'Non-Media' 
            END as Complainant_Role,
            CASE 
                WHEN Accused_Category = 'Media' THEN 'Media' 
                ELSE 'Non-Media' 
            END as Accused_Role,
            COUNT(*) as Count
        FROM "against"
        GROUP BY Complainant_Role, Accused_Role
        ORDER BY Count DESC
        """
        
        df_against = pd.read_sql_query(query_against, conn)
        f.write(df_against.to_string(index=False) + "\n")
        
        # Calculate "Swapped" candidates in 'against' table
        # Candidate = Accused is NOT Media, but Complainant IS Media
        swapped_against = df_against[
            (df_against['Accused_Role'] == 'Non-Media') & 
            (df_against['Complainant_Role'] == 'Media')
        ]['Count'].sum()
        
        total_against = df_against['Count'].sum()
        f.write(f"\nPotential Swapped Rows in 'against': {swapped_against} / {total_against} ({swapped_against/total_against*100:.1f}%)\n")

        conn.close()
        f.write("\n" + "="*80 + "\n")

if __name__ == "__main__":
    validate_hypothesis()
