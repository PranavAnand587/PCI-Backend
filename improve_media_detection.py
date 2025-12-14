import pandas as pd
import sqlite3
import os
import re

# Paths
base_path = r"d:\Projects\mphasis\pci_project_all\api_dev"
by_press_path = os.path.join(base_path, "final_by_press_with_er.csv")
against_press_path = os.path.join(base_path, "final_against_press_with_level.csv")
db_path = os.path.join(base_path, "complaints.db")

# --- 1. Enhanced Media Detection ---

MEDIA_KEYWORDS = [
    'editor', 'journalist', 'reporter', 'press', 'media', 'news', 'channel', 'paper',
    'correspondent', 'photographer', 'cameraman', 'anchor', 'producer', 'publication'
]

INDIAN_MEDIA_BRANDS = [
    'hindu', 'times', 'indian express', 'dainik', 'jagran', 'bhaskar', 'tribune',
    'hindustan', 'amar ujala', 'prabhat khabar', 'lokmat', 'eenadu', 'ananda bazar',
    'malayala manorama', 'mathrubhumi', 'sakshi', 'prajavani', 'vijay karnataka',
    'sandesh', 'gujarat samachar', 'rajasthan patrika', 'navbharat', 'jansatta',
    'zee', 'star', 'ndtv', 'india today', 'outlook', 'frontline', 'week',
    'deccan', 'pioneer', 'statesman', 'telegraph', 'asian age', 'mint',
    'economic times', 'business standard', 'financial express',
    'scroll', 'wire', 'quint', 'newslaundry'
]

def is_media_entity(text):
    if not isinstance(text, str):
        return False
    t = text.lower()
    return any(k in t for k in MEDIA_KEYWORDS) or any(b in t for b in INDIAN_MEDIA_BRANDS)


def extract_category_occupation(affiliation, force_media=False):
    if force_media:
        return "Media", "Journalist/Media"

    if not isinstance(affiliation, str):
        return "Individual", "Other"

    aff = affiliation.lower()

    # CATEGORY
    if any(x in aff for x in ['police','govt','government','ministry','department','ias','ips']):
        category = "Government"
    elif any(x in aff for x in ['bjp','congress','party','mla','mp','politician']):
        category = "Political"
    elif is_media_entity(affiliation):
        category = "Media"
    elif any(x in aff for x in ['advocate','lawyer','court']):
        category = "Professional"
    elif any(x in aff for x in ['doctor','medical','hospital']):
        category = "Professional"
    elif any(x in aff for x in ['company','ltd','pvt','corporate']):
        category = "Business"
    elif any(x in aff for x in ['ngo','society','association','activist']):
        category = "Civil Society"
    else:
        category = "Individual"

    # OCCUPATION
    if 'police' in aff:
        occupation = "Police"
    elif any(x in aff for x in ['judge','judicial']):
        occupation = "Judiciary"
    elif any(x in aff for x in ['advocate','lawyer']):
        occupation = "Legal Professional"
    elif is_media_entity(affiliation):
        occupation = "Journalist/Media"
    else:
        occupation = "Other"

    return category, occupation


def normalize_decision(decision):
    if not isinstance(decision, str):
        return "Other", str(decision)

    d = decision.lower()
    if any(x in d for x in ['upheld','censured','warned','admonished','directed']):
        return "Upheld", decision
    elif any(x in d for x in ['dismissed','closed']):
        return "Closed", decision
    elif any(x in d for x in ['disposed','settled']):
        return "Disposed", decision
    elif 'sub-judice' in d:
        return "Sub-judice", decision
    else:
        return "Other", decision


# --- 2. Processing ---

def process_data():
    df_against = pd.read_csv(against_press_path, sep=';', on_bad_lines='skip')
    df_by = pd.read_csv(by_press_path, sep=';', on_bad_lines='skip')

    # ---------------- AGAINST PRESS ----------------
    df_against['Complainant'] = df_against['c_name_resolved'].fillna(df_against['Complainant'])
    df_against['Against'] = df_against['a_name_resolved'].fillna(df_against['Against'])
    df_against['Complainant_Aff'] = df_against['c_aff_resolved'].fillna(df_against['Complainant_Aff'])
    df_against['Against_Aff'] = df_against['a_aff_resolved'].fillna(df_against['Against_Aff'])

    df_against['ComplaintType_Normalized'] = df_against['res_ComplaintType'].fillna(df_against['ComplaintType'])

    dec = df_against['Decision'].apply(normalize_decision)
    df_against['Decision_Parent'] = dec.apply(lambda x: x[0])
    df_against['Decision_Specific'] = dec.apply(lambda x: x[1])

    media_accused_types = [
        'Principles and Defamation',
        'Principles and Publications',
        'Paid News',
        'Misleading Advertisements',
        'Communal, Casteist, Anti National and Anti Religious Writings'
    ]

    df_against['force_accused_media'] = (
        df_against['ComplaintType_Normalized'].isin(media_accused_types)
        & (df_against['ComplaintType_Normalized'] != 'Suo-Motu')
    )

    df_against[['Complainant_Category','Complainant_Occupation']] = df_against.apply(
        lambda r: extract_category_occupation(r['Complainant_Aff'], False),
        axis=1, result_type='expand'
    )

    df_against[['Accused_Category','Accused_Occupation']] = df_against.apply(
        lambda r: extract_category_occupation(r['Against_Aff'], r['force_accused_media']),
        axis=1, result_type='expand'
    )

    df_against.drop(columns=['force_accused_media'], inplace=True)

    # ---------------- BY PRESS ----------------
    df_by['Complainant'] = df_by['c_name_resolved'].fillna(df_by['Complainant'])
    df_by['Against'] = df_by['a_name_resolved'].fillna(df_by['Against'])
    df_by['Complainant_Aff'] = df_by['c_aff_resolved'].fillna(df_by['Complainant_Aff'])
    df_by['Against_Aff'] = df_by['a_aff_resolved'].fillna(df_by['Against_Aff'])

    df_by['ComplaintType_Normalized'] = df_by['ComplaintType']

    dec = df_by['Decision'].apply(normalize_decision)
    df_by['Decision_Parent'] = dec.apply(lambda x: x[0])
    df_by['Decision_Specific'] = dec.apply(lambda x: x[1])

    media_complainant_types = [
        'Harassment of Newsmen',
        'Facilities to the Press',
        'Violence against Newsmen',
        'Curtailment of Press Freedom'
    ]

    df_by['force_complainant_media'] = (
        df_by['ComplaintType_Normalized'].isin(media_complainant_types)
        & (df_by['ComplaintType_Normalized'] != 'Suo-Motu')
    )

    df_by[['Complainant_Category','Complainant_Occupation']] = df_by.apply(
        lambda r: extract_category_occupation(r['Complainant_Aff'], r['force_complainant_media']),
        axis=1, result_type='expand'
    )

    df_by[['Accused_Category','Accused_Occupation']] = df_by.apply(
        lambda r: extract_category_occupation(r['Against_Aff'], False),
        axis=1, result_type='expand'
    )

    df_by.drop(columns=['force_complainant_media'], inplace=True)

    # ---------------- SAVE ----------------
    conn = sqlite3.connect(db_path)
    df_against.to_sql('against', conn, if_exists='replace', index=False)
    df_by.to_sql('by', conn, if_exists='replace', index=False)
    conn.close()

    print("Database updated successfully.")

if __name__ == "__main__":
    process_data()
