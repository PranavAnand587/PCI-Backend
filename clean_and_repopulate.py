import pandas as pd
import sqlite3
import os
import re

import pandas as pd
import sqlite3
import os
import re

# Paths
base_path = r"d:\Projects\mphasis\pci_project_all\api_dev"
by_press_path = os.path.join(base_path, "final_by_press_with_er.csv")
against_press_path = os.path.join(base_path, "final_against_press_with_level.csv")
db_path = os.path.join(base_path, "complaints.db")

# --- 1. Normalization Functions ---

def normalize_decision(decision):
    """
    Normalizes the decision into a parent category and a specific outcome.
    """
    if not isinstance(decision, str):
        return "Other", str(decision)
    
    decision_lower = decision.lower()
    
    # Define Parent Categories
    if any(x in decision_lower for x in ['upheld', 'censured', 'warned', 'admonished', 'directed']):
        return "Upheld", decision
    elif any(x in decision_lower for x in ['closed', 'dismissed']):
        return "Closed", decision
    elif any(x in decision_lower for x in ['disposed', 'settled']):
        return "Disposed", decision
    elif 'sub-judice' in decision_lower:
        return "Sub-judice", decision
    else:
        return "Other", decision

def extract_category_occupation(affiliation):
    """
    Extracts a broad Category and a specific Occupation from the affiliation string.
    """
    if not isinstance(affiliation, str):
        return "Individual", "Other"
    
    aff_lower = affiliation.lower()

    # ----- CATEGORY -----
    if any(x in aff_lower for x in ['police','govt','government','ministry','department','magistrate','official','ias','ips']):
        category = "Government"
    elif any(x in aff_lower for x in ['bjp','congress','party','mla','mp','politician','leader']):
        category = "Political"
    elif any(x in aff_lower for x in ['editor','journalist','reporter','press','media','news','channel','paper']):
        category = "Media"
    elif any(x in aff_lower for x in ['advocate','lawyer','legal','court']):
        category = "Professional"
    elif any(x in aff_lower for x in ['doctor','medical','hospital']):
        category = "Professional"
    elif any(x in aff_lower for x in ['manager','owner','director','company','ltd','pvt','corporate']):
        category = "Business"
    elif any(x in aff_lower for x in ['ngo','society','association','union','activist']):
        category = "Civil Society"
    else:
        category = "Individual"

    # ----- OCCUPATION -----
    if any(x in aff_lower for x in ['police','sho','dgp','sp']):
        occupation = "Police"
    elif any(x in aff_lower for x in ['magistrate','dm','sdm','collector','commissioner','official','secretary']):
        occupation = "Official/Administrator"
    elif any(x in aff_lower for x in ['mla','mp','minister','politician','party','leader','worker']):
        occupation = "Politician"
    elif any(x in aff_lower for x in ['judge','court','judicial']):
        occupation = "Judiciary"
    elif any(x in aff_lower for x in ['railway','defence','army']):
        occupation = "Defence/Railways"
    elif any(x in aff_lower for x in ['advocate','lawyer']):
        occupation = "Legal Professional"
    elif any(x in aff_lower for x in ['doctor','medical']):
        occupation = "Medical Professional"
    elif any(x in aff_lower for x in ['principal','teacher','professor','school','college']):
        occupation = "Education"
    elif any(x in aff_lower for x in ['editor','journalist','reporter','correspondent']):
        occupation = "Journalist/Media"
    elif any(x in aff_lower for x in ['manager','owner','proprietor','director','business']):
        occupation = "Business/Corporate"
    elif 'activist' in aff_lower or 'social worker' in aff_lower or 'ngo' in aff_lower:
        occupation = "Social Worker/Activist"
    else:
        occupation = "Other"

    return category, occupation

# --- 2. Data Loading & Processing ---

def process_data():
    print("Loading CSVs...")
    # Load CSVs with semicolon separator
    df_against = pd.read_csv(against_press_path, sep=';', on_bad_lines='skip')
    df_by = pd.read_csv(by_press_path, sep=';', on_bad_lines='skip')
    
    print(f"Loaded 'against': {len(df_against)} rows")
    print(f"Loaded 'by': {len(df_by)} rows")
    
    # --- Process 'Against Press' ---
    print("Processing 'Against Press' data...")
    
    # 1. Use Resolved Columns (Overwrite raw columns or fillna)
    # Names
    df_against['Complainant'] = df_against['c_name_resolved'].fillna(df_against['Complainant'])
    df_against['Against'] = df_against['a_name_resolved'].fillna(df_against['Against'])
    
    # Affiliations
    df_against['Complainant_Aff'] = df_against['c_aff_resolved'].fillna(df_against['Complainant_Aff'])
    df_against['Against_Aff'] = df_against['a_aff_resolved'].fillna(df_against['Against_Aff'])
    
    # Locations (Ensure resolved columns are present, maybe update Locations_Mapped if needed)
    # We'll keep the resolved columns as is in the DB
    
    # 2. Complaint Type: Use existing 'res_ComplaintType'
    df_against['ComplaintType_Normalized'] = df_against['res_ComplaintType'].fillna(df_against['ComplaintType'])
    
    # 3. Decision Hierarchy
    decision_data = df_against['Decision'].apply(normalize_decision)
    df_against['Decision_Parent'] = decision_data.apply(lambda x: x[0])
    df_against['Decision_Specific'] = decision_data.apply(lambda x: x[1])
    
    # 4. Affiliations (Complainant & Accused) - Derived from the UPDATED Complainant_Aff/Against_Aff
    # Complainant
    comp_aff_data = df_against['Complainant_Aff'].apply(extract_category_occupation)
    df_against['Complainant_Category'] = comp_aff_data.apply(lambda x: x[0])
    df_against['Complainant_Occupation'] = comp_aff_data.apply(lambda x: x[1])
    
    # Accused (Against_Aff)
    acc_aff_data = df_against['Against_Aff'].apply(extract_category_occupation)
    df_against['Accused_Category'] = acc_aff_data.apply(lambda x: x[0])
    df_against['Accused_Occupation'] = acc_aff_data.apply(lambda x: x[1])
    
    
    # --- Process 'By Press' ---
    print("Processing 'By Press' data...")
    
    # 1. Use Resolved Columns
    # Names
    df_by['Complainant'] = df_by['c_name_resolved'].fillna(df_by['Complainant'])
    df_by['Against'] = df_by['a_name_resolved'].fillna(df_by['Against'])
    
    # Affiliations
    df_by['Complainant_Aff'] = df_by['c_aff_resolved'].fillna(df_by['Complainant_Aff'])
    df_by['Against_Aff'] = df_by['a_aff_resolved'].fillna(df_by['Against_Aff'])
    
    # 2. Complaint Type: Use raw 'ComplaintType' as 'by' press doesn't have 'res_ComplaintType'
    df_by['ComplaintType_Normalized'] = df_by['ComplaintType']
    
    # 3. Decision Hierarchy
    decision_data_by = df_by['Decision'].apply(normalize_decision)
    df_by['Decision_Parent'] = decision_data_by.apply(lambda x: x[0])
    df_by['Decision_Specific'] = decision_data_by.apply(lambda x: x[1])
    
    # 4. Affiliations - Derived from UPDATED columns
    # Complainant
    comp_aff_data_by = df_by['Complainant_Aff'].apply(extract_category_occupation)
    df_by['Complainant_Category'] = comp_aff_data_by.apply(lambda x: x[0])
    df_by['Complainant_Occupation'] = comp_aff_data_by.apply(lambda x: x[1])
    
    # Accused
    acc_aff_data_by = df_by['Against_Aff'].apply(extract_category_occupation)
    df_by['Accused_Category'] = acc_aff_data_by.apply(lambda x: x[0])
    df_by['Accused_Occupation'] = acc_aff_data_by.apply(lambda x: x[1])

    
    # --- 3. Save to Database ---
    print("Saving to database...")
    conn = sqlite3.connect(db_path)
    
    # Replace existing tables
    df_against.to_sql('against', conn, if_exists='replace', index=False)
    df_by.to_sql('by', conn, if_exists='replace', index=False)
    
    conn.close()
    print("Database updated successfully!")

if __name__ == "__main__":
    process_data()
