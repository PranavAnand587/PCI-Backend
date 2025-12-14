import sqlite3
import pandas as pd
import os

# Set paths
base_path = r"d:\Projects\mphasis\pci_project_all\api_dev"
db_path = os.path.join(base_path, "complaints.db")

def analyze_complaint_types():
    with open("analysis_results.txt", "w", encoding="utf-8") as f:
        f.write("="*80 + "\n")
        f.write("ANALYZING COMPLAINT TYPES vs ROLE CLASSIFICATION\n")
        f.write("="*80 + "\n")
        
        conn = sqlite3.connect(db_path)
        
        # ---------------------------------------------------------
        # 1. Analyze 'by' table (Threats)
        # ---------------------------------------------------------
        f.write("\n--- 'by' Table: Complaint Types for 'Non-Media vs Non-Media' cases ---\n")
        f.write("(These are the 632 cases where we missed identifying the Journalist)\n\n")
        
        query_by_missing = """
        SELECT 
            ComplaintType_Normalized,
            COUNT(*) as Count
        FROM "by"
        WHERE 
            (CASE WHEN Complainant_Category = 'Media' THEN 1 ELSE 0 END) = 0
            AND
            (CASE WHEN Accused_Category = 'Media' THEN 1 ELSE 0 END) = 0
        GROUP BY ComplaintType_Normalized
        ORDER BY Count DESC
        LIMIT 20
        """
        df_by_missing = pd.read_sql_query(query_by_missing, conn)
        f.write(df_by_missing.to_string(index=False) + "\n")

        f.write("\n\n--- 'by' Table: Complaint Types for 'Swapped' cases (Non-Media vs Media) ---\n")
        f.write("(These are the 4 cases where Complainant=Non-Media, Accused=Media)\n\n")
        
        query_by_swapped = """
        SELECT 
            ComplaintType_Normalized,
            Complainant,
            Against,
            COUNT(*) as Count
        FROM "by"
        WHERE 
            Complainant_Category != 'Media'
            AND
            Accused_Category = 'Media'
        GROUP BY ComplaintType_Normalized, Complainant, Against
        ORDER BY Count DESC
        """
        df_by_swapped = pd.read_sql_query(query_by_swapped, conn)
        f.write(df_by_swapped.to_string(index=False) + "\n")


        # ---------------------------------------------------------
        # 2. Analyze 'against' table (Complaints)
        # ---------------------------------------------------------
        f.write("\n\n" + "="*40 + "\n")
        f.write("\n--- 'against' Table: Complaint Types for 'Non-Media vs Non-Media' cases ---\n")
        f.write("(These are the 4460 cases where we missed identifying the Media Accused)\n\n")
        
        query_against_missing = """
        SELECT 
            ComplaintType_Normalized,
            COUNT(*) as Count
        FROM "against"
        WHERE 
            (CASE WHEN Complainant_Category = 'Media' THEN 1 ELSE 0 END) = 0
            AND
            (CASE WHEN Accused_Category = 'Media' THEN 1 ELSE 0 END) = 0
        GROUP BY ComplaintType_Normalized
        ORDER BY Count DESC
        LIMIT 20
        """
        df_against_missing = pd.read_sql_query(query_against_missing, conn)
        f.write(df_against_missing.to_string(index=False) + "\n")
        
        
        f.write("\n\n--- 'against' Table: Complaint Types for 'Swapped' cases (Media vs Non-Media) ---\n")
        f.write("(These are the 132 cases where Complainant=Media, Accused=Non-Media)\n\n")
        
        query_against_swapped = """
        SELECT 
            ComplaintType_Normalized,
            COUNT(*) as Count
        FROM "against"
        WHERE 
            Complainant_Category = 'Media'
            AND
            Accused_Category != 'Media'
        GROUP BY ComplaintType_Normalized
        ORDER BY Count DESC
        LIMIT 20
        """
        df_against_swapped = pd.read_sql_query(query_against_swapped, conn)
        f.write(df_against_swapped.to_string(index=False) + "\n")

        conn.close()
        f.write("\n" + "="*80 + "\n")

if __name__ == "__main__":
    analyze_complaint_types()
