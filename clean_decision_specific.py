import sqlite3
import re
import sys
import codecs

# Set stdout encoding for proper output
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

def clean_decision_specific(value):
    """
    Clean and normalize a Decision_Specific value.
    Returns the cleaned value or None if it should be mapped to "Other".
    """
    if value is None:
        return "Other"
    
    original = value
    
    # Step 1: Basic whitespace cleanup
    value = value.strip()
    
    # Step 2: Handle completely empty or whitespace-only
    if not value:
        return "Other"
    
    # Step 3: Map known fragments and anomalies to "Other"
    fragments_to_other = {
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',  # Single letters
        '(a)', '(b)', '(c)', '(d)', '(e)', '(f)',  # Parenthesized letters
        'nan', 'default', 'in default',  # Meaningless values
        'devoid of merit',  # Fragment (should be part of "Dismissed - Devoid of merit")
        'rejoinder', 'ventilated',  # Single word fragments
        'management proceedings dropped-assurance by newspaper',  # Malformed
    }
    
    if value.lower() in fragments_to_other:
        return "Other"
    
    # Step 4: Check if it's just a number or date
    if re.match(r'^[\d,\s]+$', value):
        return "Other"
    
    # Step 5: Handle spaced-out words like "p u r s u a n c e"
    if re.match(r'^[a-z](\s[a-z])+$', value.lower()):
        return "Other"
    
    # Step 6: Map incomplete fragments to proper values
    fragment_mappings = {
        'pursued': 'Not Pursued',
        'to rest': 'Matter Allowed to Rest',
        'directions': 'Directions',
        'observations': 'Observations',
        'clarification': 'Clarification Published',
        'dropped': 'Dropped',
        'with advise': 'Disposed of with Advise',
        'with direction': 'Disposed of with Direction',
        'with directions': 'Disposed of with Directions',
        'disposed of with directions': 'Disposed of with Directions',
        'settled': 'Settled',
    }
    
    if value.lower() in fragment_mappings:
        return fragment_mappings[value.lower()]
    
    # Step 7: Remove incomplete endings like "Dismissed of" (but keep "Dismissed off")
    # If it ends with " of" and is not "Disposed of" or "Disposed off", it's likely incomplete
    if value.endswith(' of') and not value.lower().startswith('disposed of'):
        return "Other"
    
    # Step 8: Replace en-dash with hyphen
    value = value.replace('–', '-')
    value = value.replace('—', '-')
    
    # Step 9: Normalize multiple spaces to single space
    value = re.sub(r'\s+', ' ', value)
    
    # Step 10: Remove trailing period
    value = value.rstrip('.')
    
    # Step 11: Normalize case - create canonical form
    # We'll use a mapping of normalized (lowercase) to canonical form
    canonical_forms = {
        'm': 'Other', # Single letter M
        'lack of substance': 'Closed for Lack of Substance',
        'lack ofsubstancе': 'Closed for Lack of Substance', # Typo fix
        'dismissed': 'Dismissed',
        'upheld': 'Upheld',
        'directions': 'Directions',
        'sub-judice': 'Sub-judice',
        'disposed of with directions': 'Disposed of with Directions',
        'disposed of': 'Disposed of',
        'dismissed on merits': 'Dismissed on Merits',
        'dismissed on merit': 'Dismissed on Merit',
        'disposed with observations': 'Disposed with Observations',
        'withdrawn': 'Withdrawn',
        'assurance': 'Assurance',
        'disposed of with direction': 'Disposed of with Direction',
        'proceedings dropped': 'Proceedings Dropped',
        'disposed with directions': 'Disposed with Directions',
        'disposed of with observation': 'Disposed of with Observation',
        'closed with directions': 'Closed with Directions',
        'closed with observations': 'Closed with Observations',
        'dismissed for lack of substance': 'Dismissed for Lack of Substance',
        'disposed off': 'Disposed off',
        'assurance by authorities': 'Assurance by Authorities',
        'disposed of with observations': 'Disposed of with Observations',
        'disposed off with assurance': 'Disposed off with Assurance',
        'rejected': 'Rejected',
        'dismissed with observations': 'Dismissed with Observations',
        'disposed off with observations': 'Disposed off with Observations',
        'disposed': 'Disposed',
        'disposed off - no action': 'Disposed off - No Action',
        'disposed upon assurance': 'Disposed upon Assurance',
        'charges not substantiated': 'Charges not Substantiated',
        'dismissed with observation': 'Dismissed with Observation',
        'dismissed devoid of merits': 'Dismissed Devoid of Merits',
        'settled': 'Settled',
        'disposed of with direction': 'Disposed of with Direction',
        'upheld (censured)': 'Upheld (Censured)',
        'withdrawn': 'Withdrawn',
        'directions to publish clarification': 'Directions to Publish Clarification',
        'contradiction directed': 'Contradiction Directed',
        'upheld (warned)': 'Upheld (Warned)',
        'dismissed for non-pursuance': 'Dismissed for Non-pursuance',
        'no action warranted': 'No Action Warranted',
        'dismissed for non-prosecution': 'Dismissed for Non-prosecution',
        'upheld (displeasure)': 'Upheld (Displeasure)',
        'admonished and censured': 'Admonished and Censured',
        'dismissed being devoid of merit': 'Dismissed being Devoid of Merit',
        'disposed of with observation': 'Disposed of with Observation',
        'dismissed - no violation of norms of journalistic conduct': 'Dismissed - No Violation of Norms of Journalistic Conduct',
        'disposed off with directions': 'Disposed off with Directions',
        'dismissed the matter for default': 'Dismissed the Matter for Default',
        'disposed of being sub-judice': 'Disposed of being Sub-judice',
        'closed': 'Closed',
        'dismissed the matter with default': 'Dismissed the Matter with Default',
        'disposed off with observation': 'Disposed off with Observation',
        'disposed off with direction': 'Disposed off with Direction',
        'disapproval': 'Disapproval',
        'advise': 'Advise',
        'disposed off - sub - judice': 'Disposed off - Sub-judice',
        'directions (upheld)': 'Directions (Upheld)',
        'matter allowed to rest': 'Matter Allowed to Rest',
        'strong disapproval': 'Strong Disapproval',
        'settlement': 'Settlement',
        'observations': 'Observations',
        'disposed off sub judice': 'Disposed off Sub-judice',
        'dismissed with directions': 'Dismissed with Directions',
        'dismissed being baseless': 'Dismissed being Baseless',
        'matter settled': 'Matter Settled',
        'grievance redressed': 'Grievance Redressed',
        'dispose of with observation': 'Dispose of with Observation',
        'dismissed - sub- judice': 'Dismissed - Sub-judice',
        'dismissed - no action': 'Dismissed - No Action',
        'dismissed for non-prosecution': 'Dismissed for Non-prosecution',
        'upheld with observations': 'Upheld with Observations',
        'dropped being sub-judice': 'Dropped being Sub-judice',
        'disposed with direction': 'Disposed with Direction',
        'disposed with obser ations (upheld)': 'Disposed with Observations (Upheld)',
        'disposed upon assurance': 'Disposed upon Assurance',
        'disposed off - no action': 'Disposed off - No Action',
        'dismissed - devoid of merits': 'Dismissed - Devoid of Merits',
        'dismissed devoid of merit': 'Dismissed Devoid of Merit',
        'closed for non-prosecution': 'Closed for Non-prosecution',
        'caution issued to authorities': 'Caution Issued to Authorities',
        'regret expressed': 'Regret Expressed',
        'outside jurisdiction': 'Outside Jurisdiction',
        'no merits - dismissed': 'No Merits - Dismissed',
        'grievance redressed (settlement)': 'Grievance Redressed (Settlement)',
        'dropped for non-prosecution': 'Dropped for Non-prosecution',
        'dropped': 'Dropped',
        'disposed with sub - judice': 'Disposed with Sub-judice',
        'disposed off - no action': 'Disposed off - No Action',
        'disposed of the matter for default': 'Disposed of the Matter for Default',
        'disposed of on assurance given by police': 'Disposed of on Assurance given by Police',
        'disposed of being lack of substance': 'Disposed of being Lack of Substance',
        'dismissed- devoid of merits': 'Dismissed - Devoid of Merits',
        'dismissed - settled': 'Dismissed - Settled',
        'dismissed being outside jurisdiction': 'Dismissed being Outside Jurisdiction',
        'dismissed being devoid of substance': 'Dismissed being Devoid of Substance',
        'complaint not pursued': 'Complaint not Pursued',
        'complain not sustained': 'Complaint not Sustained',
        'closed on assurance': 'Closed on Assurance',
        'closed for lack of substance': 'Closed for Lack of Substance',
        'not pursued': 'Not Pursued',
    }
    
    # Check if we have a canonical form for this value
    value_lower = value.lower()
    if value_lower in canonical_forms:
        return canonical_forms[value_lower]
    
    # Step 12: If no canonical form exists, ensure first letter is capitalized
    if value and value[0].islower():
        value = value[0].upper() + value[1:]
    
    return value


def main():
    print("="*80)
    print("CLEANING Decision_Specific COLUMN")
    print("="*80)
    
    conn = sqlite3.connect('complaints.db')
    cursor = conn.cursor()
    
    for table in ['against', 'by']:
        print(f"\n--- Processing table: {table} ---")
        
        # Step 1: Create backup column if it doesn't exist
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'Decision_Specific_backup' not in columns:
            print(f"Creating backup column Decision_Specific_backup...")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN Decision_Specific_backup TEXT")
            cursor.execute(f"UPDATE {table} SET Decision_Specific_backup = Decision_Specific")
            conn.commit()
            print("Backup created.")
        else:
            print("Backup column already exists.")
        
        # Step 2: Get all unique values before cleaning
        cursor.execute(f"SELECT COUNT(DISTINCT Decision_Specific) FROM {table}")
        before_count = cursor.fetchone()[0]
        print(f"Unique values before cleaning: {before_count}")
        
        # Step 3: Clean all values
        cursor.execute(f"SELECT ROWID, Decision_Specific FROM {table}")
        rows = cursor.fetchall()
        
        cleaned_count = 0
        for rowid, value in rows:
            cleaned = clean_decision_specific(value)
            if cleaned != value:
                cursor.execute(f"UPDATE {table} SET Decision_Specific = ? WHERE ROWID = ?", (cleaned, rowid))
                cleaned_count += 1
        
        conn.commit()
        print(f"Cleaned {cleaned_count} rows.")
        
        # Step 4: Get all unique values after cleaning
        cursor.execute(f"SELECT COUNT(DISTINCT Decision_Specific) FROM {table}")
        after_count = cursor.fetchone()[0]
        print(f"Unique values after cleaning: {after_count}")
        print(f"Reduction: {before_count - after_count} unique values consolidated")
        
        # Step 5: Show top 20 most common values after cleaning
        print("\nTop 20 most common values after cleaning:")
        cursor.execute(f"""
            SELECT Decision_Specific, COUNT(*) as cnt 
            FROM {table} 
            GROUP BY Decision_Specific 
            ORDER BY cnt DESC 
            LIMIT 20
        """)
        for val, cnt in cursor.fetchall():
            print(f"  [{cnt}] {repr(val)}")
    
    conn.close()
    print("\n" + "="*80)
    print("CLEANING COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
