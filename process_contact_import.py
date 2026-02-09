#!/usr/bin/env python3
"""
Process HubSpot Contact Import sheet:
1. Set HubSpot Company Association = hubspot_record_id from Matched Facilities (matched by address)
2. Set ClickUp Company Association = clickup_task_id (extracted from URL)
3. Split name column into First Name and Last Name
4. Check BigQuery HubSpot contacts for existing contacts by first name, last name, email
"""

import os
import sys
import re
import pandas as pd
from typing import Optional, Tuple

# BigQuery imports
try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

# =========================
# BigQuery Configuration
# =========================
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'gen-lang-client-0844868008')
BQ_LOCATION = os.getenv('BQ_LOCATION', 'US')
HUBSPOT_CONTACTS_TABLE = f'{BQ_PROJECT_ID}.HubSpot_Airbyte.contacts'

BQ_CLIENT = None


def init_bigquery_client() -> bigquery.Client:
    """Initialize BigQuery client with authentication."""
    global BQ_CLIENT
    if not BIGQUERY_AVAILABLE:
        print("✗ ERROR: BigQuery libraries not installed", file=sys.stderr)
        sys.exit(1)
    
    if not BQ_CLIENT:
        BQ_CLIENT = bigquery.Client(project=BQ_PROJECT_ID, location=BQ_LOCATION)
    return BQ_CLIENT


def extract_clickup_task_id(url: str) -> str:
    """Extract task ID from ClickUp URL.
    Example: https://app.clickup.com/t/86a2vwzc1 -> 86a2vwzc1
    """
    if pd.isna(url) or not url:
        return ''
    match = re.search(r'/t/([a-zA-Z0-9]+)', str(url))
    return match.group(1) if match else ''


def split_name(full_name: str) -> Tuple[str, str]:
    """Split full name into first and last name."""
    if pd.isna(full_name) or not full_name:
        return '', ''
    parts = str(full_name).strip().split()
    if len(parts) == 1:
        return parts[0], ''
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        # Handle middle names - first name + rest as last name
        return parts[0], ' '.join(parts[1:])


def load_hubspot_contacts() -> Tuple[pd.DataFrame, dict, dict]:
    """Load all HubSpot contacts into memory for fast lookup."""
    if not BIGQUERY_AVAILABLE:
        return pd.DataFrame(), {}, {}
    
    client = init_bigquery_client()
    
    query = f"""
    SELECT 
        id as contact_record_id,
        properties_firstname as hs_first_name,
        properties_lastname as hs_last_name,
        properties_email as hs_email,
        properties_company as hs_company_name
    FROM `{HUBSPOT_CONTACTS_TABLE}`
    WHERE properties_email IS NOT NULL 
       OR (properties_firstname IS NOT NULL AND properties_lastname IS NOT NULL)
    """
    
    try:
        print("  Loading HubSpot contacts from BigQuery...")
        df = client.query(query).to_dataframe()
        print(f"  ✓ Loaded {len(df)} HubSpot contacts")
        
        # Build lookup indexes
        email_lookup = {}
        name_lookup = {}
        
        for _, row in df.iterrows():
            email = str(row['hs_email']).lower().strip() if pd.notna(row['hs_email']) else ''
            first = str(row['hs_first_name']).lower().strip() if pd.notna(row['hs_first_name']) else ''
            last = str(row['hs_last_name']).lower().strip() if pd.notna(row['hs_last_name']) else ''
            
            if email:
                email_lookup[email] = {
                    'contact_record_id': row['contact_record_id'],
                    'hs_first_name': row['hs_first_name'],
                    'hs_last_name': row['hs_last_name'],
                    'hs_company_name': row['hs_company_name']
                }
            
            if first and last:
                name_key = f"{first}|{last}"
                name_lookup[name_key] = {
                    'contact_record_id': row['contact_record_id'],
                    'hs_first_name': row['hs_first_name'],
                    'hs_last_name': row['hs_last_name'],
                    'hs_company_name': row['hs_company_name']
                }
        
        return df, email_lookup, name_lookup
        
    except Exception as e:
        print(f"  Warning: Error loading HubSpot contacts: {e}")
        return pd.DataFrame(), {}, {}


def check_hubspot_contact_exists(email_lookup: dict, name_lookup: dict, 
                                  first_name: str, last_name: str, email: str) -> Optional[dict]:
    """Check if contact exists using in-memory lookups."""
    # Check by email first (most reliable)
    if email:
        email_key = str(email).lower().strip()
        if email_key in email_lookup:
            return email_lookup[email_key]
    
    # Check by name
    if first_name and last_name:
        name_key = f"{str(first_name).lower().strip()}|{str(last_name).lower().strip()}"
        if name_key in name_lookup:
            return name_lookup[name_key]
    
    return None


def main():
    excel_file = 'PACS employees and facilities.xlsx'
    contact_sheet = 'HubSpot Contact Import'
    facilities_sheet = 'Matched Facilities Final Clean'
    output_sheet = 'HubSpot Contacts Processed'
    
    print("=" * 60)
    print("HubSpot Contact Import Processing")
    print("=" * 60)
    
    # Step 1: Read sheets
    print(f"\n[1/6] Reading Excel sheets...")
    try:
        contact_df = pd.read_excel(excel_file, sheet_name=contact_sheet)
        facilities_df = pd.read_excel(excel_file, sheet_name=facilities_sheet)
        print(f"  ✓ Loaded {len(contact_df)} contacts from '{contact_sheet}'")
        print(f"  ✓ Loaded {len(facilities_df)} facilities from '{facilities_sheet}'")
    except Exception as e:
        print(f"  ✗ Error reading Excel: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Step 2: Create address lookup from Matched Facilities
    print(f"\n[2/6] Building address lookup...")
    address_lookup = {}
    for _, row in facilities_df.iterrows():
        addr = str(row.get('unique_address', '')).strip() if pd.notna(row.get('unique_address')) else ''
        if addr:
            address_lookup[addr] = {
                'hubspot_record_id': row.get('hubspot_record_id', ''),
                'clickup_task_url': row.get('clickup_task_url', ''),
                'hubspot_company_name': row.get('hubspot_company_name', '')
            }
    print(f"  ✓ Built lookup for {len(address_lookup)} addresses")
    
    # Step 3: Load HubSpot contacts for fast lookup
    print(f"\n[3/6] Loading HubSpot contacts from BigQuery...")
    _, email_lookup, name_lookup = load_hubspot_contacts()
    
    # Step 4: Process each contact
    print(f"\n[4/6] Processing contacts...")
    
    # Initialize new columns
    contact_df['HubSpot Company Association'] = ''
    contact_df['ClickUp Company Association'] = ''
    contact_df['First Name'] = ''
    contact_df['Last Name'] = ''
    contact_df['HubSpot Contact Record ID'] = ''
    contact_df['HS Contact First Name'] = ''
    contact_df['HS Contact Last Name'] = ''
    contact_df['HS Contact Company'] = ''
    
    matched_count = 0
    existing_contacts = 0
    
    for idx, row in contact_df.iterrows():
        if idx % 100 == 0:
            print(f"  Processing {idx}/{len(contact_df)}...")
        
        # Get address and match to facilities
        addr = str(row.get('address', '')).strip() if pd.notna(row.get('address')) else ''
        if addr in address_lookup:
            matched_count += 1
            match_data = address_lookup[addr]
            
            # Set HubSpot Company Association
            contact_df.at[idx, 'HubSpot Company Association'] = match_data['hubspot_record_id']
            
            # Extract and set ClickUp task ID
            task_id = extract_clickup_task_id(match_data['clickup_task_url'])
            contact_df.at[idx, 'ClickUp Company Association'] = task_id
        
        # Split name into First Name and Last Name
        full_name = row.get('name', '')
        first_name, last_name = split_name(full_name)
        contact_df.at[idx, 'First Name'] = first_name
        contact_df.at[idx, 'Last Name'] = last_name
        
        # Check if contact exists in HubSpot using in-memory lookup
        email = str(row.get('emails', '')).strip() if pd.notna(row.get('emails')) else ''
        hs_contact = check_hubspot_contact_exists(email_lookup, name_lookup, first_name, last_name, email)
        if hs_contact:
            existing_contacts += 1
            contact_df.at[idx, 'HubSpot Contact Record ID'] = hs_contact['contact_record_id']
            contact_df.at[idx, 'HS Contact First Name'] = hs_contact['hs_first_name']
            contact_df.at[idx, 'HS Contact Last Name'] = hs_contact['hs_last_name']
            contact_df.at[idx, 'HS Contact Company'] = hs_contact['hs_company_name']
    
    print(f"  ✓ Matched {matched_count} contacts to facilities")
    print(f"  ✓ Found {existing_contacts} existing contacts in HubSpot")
    
    # Step 5: Deduplicate contacts
    print(f"\n[5/6] Deduplicating contacts...")
    original_count = len(contact_df)
    
    # Create deduplication key from facility, first name, last name, and emails
    contact_df['dedup_key'] = (
        contact_df['facility'].fillna('').str.lower().str.strip() + '|' +
        contact_df['First Name'].fillna('').str.lower().str.strip() + '|' +
        contact_df['Last Name'].fillna('').str.lower().str.strip() + '|' +
        contact_df['emails'].fillna('').str.lower().str.strip()
    )
    
    # Remove duplicates, keeping the first occurrence
    contact_df = contact_df.drop_duplicates(subset=['dedup_key'], keep='first')
    
    # Remove the temporary dedup key column
    contact_df = contact_df.drop(columns=['dedup_key'])
    
    deduped_count = len(contact_df)
    removed_count = original_count - deduped_count
    print(f"  ✓ Removed {removed_count} duplicates, {deduped_count} unique contacts remaining")
    
    # Step 6: Save results
    print(f"\n[6/6] Saving results to Excel...")
    try:
        with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            contact_df.to_excel(writer, sheet_name=output_sheet, index=False)
        print(f"  ✓ Results saved to '{output_sheet}' sheet in {excel_file}")
    except Exception as e:
        print(f"  ✗ Error saving: {e}", file=sys.stderr)
    
    # Summary
    print("\n" + "=" * 60)
    print("Processing Summary")
    print("=" * 60)
    print(f"Total contacts processed: {len(contact_df)}")
    print(f"Address matches: {matched_count}")
    print(f"Existing HubSpot contacts found: {existing_contacts}")
    print(f"New contacts: {len(contact_df) - existing_contacts}")
    print("=" * 60)


if __name__ == '__main__':
    main()
