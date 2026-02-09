#!/usr/bin/env python3
"""
Match unique addresses from Excel with HubSpot companies and ClickUp tasks.
Uses BOTH address AND facility name for fuzzy matching.
Outputs: company name, hubspot record id, hubspot full address, task name, task location
PLUS: HubSpot parent company name and ClickUp Corporations list task
"""

import os
import sys
import re
import json
import ast
from typing import Optional, List, Dict, Tuple
from collections import defaultdict
import pandas as pd

# Try to use rapidfuzz for faster matching, fall back to difflib
try:
    from rapidfuzz import fuzz, process
    USE_RAPIDFUZZ = True
except ImportError:
    from difflib import SequenceMatcher
    USE_RAPIDFUZZ = False

# BigQuery imports
try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound, BadRequest, Forbidden
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

# =========================
# BigQuery Configuration
# =========================
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID', 'gen-lang-client-0844868008')
BQ_LOCATION = os.getenv('BQ_LOCATION', 'US')

# Table references
HUBSPOT_TABLE = f'{BQ_PROJECT_ID}.HubSpot_Airbyte.companies'
CLICKUP_TABLE = f'{BQ_PROJECT_ID}.ClickUp_AirbyteCustom.task'

# ClickUp Corporations List ID
CORPORATIONS_LIST_ID = '901302721443'

# Global BigQuery client
BQ_CLIENT = None


def init_bigquery_client() -> bigquery.Client:
    """Initialize BigQuery client with authentication."""
    global BQ_CLIENT

    if not BIGQUERY_AVAILABLE:
        print("✗ ERROR: BigQuery libraries not installed", file=sys.stderr)
        sys.exit(1)

    try:
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path and os.path.exists(creds_path):
            print(f"  ✓ Using service account from: {creds_path}")
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(creds_path)
            BQ_CLIENT = bigquery.Client(
                project=BQ_PROJECT_ID,
                credentials=credentials,
                location=BQ_LOCATION
            )
            return BQ_CLIENT

        sa_json = os.getenv('BQ_SERVICE_ACCOUNT_JSON')
        if sa_json:
            print("  ✓ Using service account from BQ_SERVICE_ACCOUNT_JSON")
            import json as json_lib
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                json_lib.loads(sa_json)
            )
            BQ_CLIENT = bigquery.Client(
                project=BQ_PROJECT_ID,
                credentials=credentials,
                location=BQ_LOCATION
            )
            return BQ_CLIENT

        print("  ✓ Using application default credentials")
        BQ_CLIENT = bigquery.Client(project=BQ_PROJECT_ID, location=BQ_LOCATION)
        list(BQ_CLIENT.list_datasets(max_results=1))
        print(f"  ✓ Connected to BigQuery project: {BQ_PROJECT_ID}")
        return BQ_CLIENT

    except Exception as e:
        print(f"\n  ✗ ERROR: BigQuery authentication failed: {e}", file=sys.stderr)
        sys.exit(1)


def read_from_bigquery(table_ref: str, query_filter: str = "") -> pd.DataFrame:
    """Read data from BigQuery table with optional filter."""
    if not BQ_CLIENT:
        init_bigquery_client()

    try:
        query = f"SELECT * FROM `{table_ref}`"
        if query_filter:
            query += f" WHERE {query_filter}"
        
        print(f"  ⏳ Querying: {table_ref}")
        if query_filter:
            print(f"     Filter: {query_filter}")
        query_job = BQ_CLIENT.query(query)
        df = query_job.to_dataframe()
        bytes_processed = query_job.total_bytes_processed or 0
        print(f"  ✓ Query completed: {bytes_processed:,} bytes processed")
        print(f"  ✓ Successfully loaded {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"  ✗ ERROR: Failed to read from BigQuery: {e}", file=sys.stderr)
        sys.exit(1)


def extract_state_from_address(addr: str) -> Optional[str]:
    """Extract 2-letter state code from US address."""
    if not addr:
        return None
    patterns = [
        r',\s*([A-Za-z]{2})\s+\d{5}',
        r',\s*([A-Za-z]{2})\s*,',
        r'\b([A-Za-z]{2})\s+\d{5}',
    ]
    us_states = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
                'DC', 'PR', 'VI', 'GU', 'AS', 'MP'}
    for pattern in patterns:
        match = re.search(pattern, addr)
        if match:
            state = match.group(1).upper()
            if state in us_states:
                return state
    return None


def normalize_address(addr: str) -> str:
    """Normalize address for comparison."""
    if pd.isna(addr):
        return ""
    addr = str(addr).lower().strip()
    addr = re.sub(r'\s+', ' ', addr)
    addr = re.sub(r',?\s*united states of america$', '', addr)
    addr = re.sub(r',?\s*usa$', '', addr)
    return addr.strip()


def build_hubspot_address(row: pd.Series) -> str:
    """Build full address from HubSpot company properties."""
    parts = []
    if pd.notna(row.get('properties_address')) and str(row.get('properties_address')):
        parts.append(str(row.get('properties_address')).strip())
    if pd.notna(row.get('properties_address2')) and str(row.get('properties_address2')):
        parts.append(str(row.get('properties_address2')).strip())
    
    city_state_zip = []
    if pd.notna(row.get('properties_city')) and str(row.get('properties_city')):
        city_state_zip.append(str(row.get('properties_city')).strip())
    if pd.notna(row.get('properties_state')) and str(row.get('properties_state')):
        city_state_zip.append(str(row.get('properties_state')).strip())
    if pd.notna(row.get('properties_zip')) and str(row.get('properties_zip')):
        city_state_zip.append(str(row.get('properties_zip')).strip())
    
    if city_state_zip:
        parts.append(', '.join(city_state_zip))
    
    if pd.notna(row.get('properties_country')) and str(row.get('properties_country')):
        parts.append(str(row.get('properties_country')).strip())
    
    return ', '.join(parts)


def extract_clickup_location(row: pd.Series) -> str:
    """Extract location/address from ClickUp task custom fields."""
    # Try custom_fields first - address is stored in "Location" field
    if pd.notna(row.get('custom_fields')) and str(row.get('custom_fields')):
        cf_str = str(row.get('custom_fields')).strip()
        try:
            cf_data = json.loads(cf_str)
            if isinstance(cf_data, list):
                for field in cf_data:
                    if isinstance(field, dict):
                        field_name = field.get('name', '').lower()
                        # Look for Location field with address data
                        if field_name == 'location':
                            value = field.get('value')
                            if isinstance(value, dict):
                                if value.get('formatted_address'):
                                    return str(value.get('formatted_address'))
                                if value.get('address'):
                                    return str(value.get('address'))
                            elif isinstance(value, str) and value:
                                return value
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Fallback to locations field (rarely has data)
    if pd.notna(row.get('locations')) and str(row.get('locations')):
        loc_str = str(row.get('locations')).strip()
        try:
            loc_data = json.loads(loc_str)
        except (json.JSONDecodeError, TypeError):
            try:
                loc_data = ast.literal_eval(loc_str)
            except (ValueError, SyntaxError):
                loc_data = None
        
        if loc_data:
            if isinstance(loc_data, list) and len(loc_data) > 0:
                for loc in loc_data:
                    if isinstance(loc, dict):
                        if loc.get('formatted_address'):
                            return str(loc.get('formatted_address'))
                        if loc.get('address'):
                            return str(loc.get('address'))
            elif isinstance(loc_data, dict):
                if loc_data.get('formatted_address'):
                    return str(loc_data.get('formatted_address'))
                if loc_data.get('address'):
                    return str(loc_data.get('address'))
    
    return ""


def combined_similarity_score(target_addr: str, target_facility: str, 
                              candidate_addr: str, candidate_name: str) -> float:
    """Calculate combined similarity using BOTH address and facility name."""
    if not target_addr or not candidate_addr:
        return 0.0
    
    # Address similarity (weight: 60%)
    if USE_RAPIDFUZZ:
        addr_score = fuzz.token_set_ratio(target_addr, candidate_addr) / 100.0
    else:
        addr_score = SequenceMatcher(None, normalize_address(target_addr), 
                                      normalize_address(candidate_addr)).ratio()
    
    # Facility name similarity (weight: 40%)
    facility_score = 0.0
    if target_facility and candidate_name:
        if USE_RAPIDFUZZ:
            facility_score = fuzz.token_set_ratio(target_facility, candidate_name) / 100.0
        else:
            facility_score = SequenceMatcher(None, normalize_address(target_facility), 
                                              normalize_address(candidate_name)).ratio()
    
    # Combined weighted score
    if facility_score > 0:
        return (addr_score * 0.6) + (facility_score * 0.4)
    else:
        return addr_score


def find_best_match_combined(target_addr: str, target_facility: str,
                             candidates: List[Tuple[str, str, Dict]]) -> Tuple[Optional[Dict], float]:
    """Find best match using combined address + facility name scoring."""
    best_match = None
    best_score = 0.0
    
    for addr_str, name_str, data in candidates:
        score = combined_similarity_score(target_addr, target_facility, addr_str, name_str)
        if score > best_score:
            best_score = score
            best_match = data
    
    return best_match, best_score


def main():
    excel_file = 'PACS employees and facilities.xlsx'
    output_sheet = 'Matched Facilities'
    
    print("=" * 60)
    print("Address + Facility Name Matching Script")
    print("With HubSpot Parent Company & ClickUp Corporations")
    print(f"Using: {'rapidfuzz' if USE_RAPIDFUZZ else 'difflib'}")
    print("=" * 60)
    
    # Step 1: Read unique addresses and map facility names
    print("\n[1/6] Reading unique addresses and mapping facility names...")
    try:
        unique_df = pd.read_excel(excel_file, sheet_name='Unique Addresses')
        
        # Read employees sheet to get facility mapping
        employees_df = pd.read_excel(excel_file, sheet_name='employees')
        
        # Create address -> facility mapping
        address_to_facility = {}
        for _, row in employees_df.iterrows():
            addr = str(row.get('address', '')).strip() if pd.notna(row.get('address')) else ''
            facility = str(row.get('facility', '')).strip() if pd.notna(row.get('facility')) else ''
            if addr and facility and addr not in address_to_facility:
                address_to_facility[addr] = facility
        
        print(f"  ✓ Built facility mapping for {len(address_to_facility)} addresses")
        
        # Add facility column to unique addresses
        unique_df['facility'] = unique_df['unique_address'].map(address_to_facility)
        unmatched = unique_df['facility'].isna().sum()
        print(f"  ✓ Matched {len(unique_df) - unmatched} addresses to facilities ({unmatched} unmatched)")
        
        # Save back to Excel with facility column
        with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            unique_df.to_excel(writer, sheet_name='Unique Addresses', index=False)
        print(f"  ✓ Updated 'Unique Addresses' sheet with facility column")
        
        unique_addresses = unique_df['unique_address'].tolist()
        facility_names = unique_df['facility'].fillna('').tolist()
        
    except Exception as e:
        print(f"  ✗ Error reading Excel: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Step 2: Query HubSpot companies
    print("\n[2/6] Querying HubSpot companies...")
    hs_df = read_from_bigquery(HUBSPOT_TABLE)
    
    print("  Building HubSpot address+name index by state...")
    hubspot_by_state = defaultdict(list)
    no_state_count = 0
    
    for _, row in hs_df.iterrows():
        full_addr = build_hubspot_address(row)
        company_name = str(row.get('properties_name', '')) if pd.notna(row.get('properties_name')) else ''
        parent_company = str(row.get('properties_parentconame', '')) if pd.notna(row.get('properties_parentconame')) else ''
        if full_addr:
            state = extract_state_from_address(full_addr)
            data = {
                'hubspot_record_id': row.get('id') or row.get('properties_hs_object_id'),
                'hubspot_company_name': company_name,
                'hubspot_full_address': full_addr,
                'hubspot_parent_company': parent_company,
            }
            if state:
                hubspot_by_state[state].append((full_addr, company_name, data))
            else:
                no_state_count += 1
                hubspot_by_state['__NO_STATE__'].append((full_addr, company_name, data))
    
    total_hs = sum(len(v) for v in hubspot_by_state.values())
    print(f"  ✓ Indexed {total_hs} HubSpot records across {len(hubspot_by_state)-1} states")
    
    # Step 3: Query ClickUp Corporations list
    print("\n[3/6] Querying ClickUp Corporations list...")
    corporations_filter = f"JSON_EXTRACT_SCALAR(list, '$.id') = '{CORPORATIONS_LIST_ID}'"
    corp_df = read_from_bigquery(CLICKUP_TABLE, corporations_filter)
    
    print("  Building Corporations list index (by name - no addresses)...")
    # Corporations list tasks don't have Location fields, so we match by name only
    corporations_candidates = []
    
    for _, row in corp_df.iterrows():
        task_name = str(row.get('name', '')) if pd.notna(row.get('name')) else ''
        if task_name:
            data = {
                'corporations_task_id': row.get('id'),
                'corporations_task_name': task_name,
                'corporations_task_location': '',
                'corporations_task_url': row.get('url'),
            }
            # Use task name as both location (for matching) and name
            corporations_candidates.append((task_name, task_name, data))
    
    print(f"  ✓ Indexed {len(corporations_candidates)} Corporations list tasks")
    
    # Step 4: Query all ClickUp tasks for facility matching
    print("\n[4/6] Querying ClickUp tasks...")
    cu_df = read_from_bigquery(CLICKUP_TABLE)
    
    print("  Building ClickUp location+name index by state...")
    clickup_by_state = defaultdict(list)
    no_state_count_cu = 0
    
    for _, row in cu_df.iterrows():
        location = extract_clickup_location(row)
        task_name = str(row.get('name', '')) if pd.notna(row.get('name')) else ''
        if location:
            state = extract_state_from_address(location)
            data = {
                'clickup_task_id': row.get('id'),
                'clickup_task_name': task_name,
                'clickup_task_location': location,
                'clickup_url': row.get('url'),
            }
            if state:
                clickup_by_state[state].append((location, task_name, data))
            else:
                no_state_count_cu += 1
                clickup_by_state['__NO_STATE__'].append((location, task_name, data))
    
    total_cu = sum(len(v) for v in clickup_by_state.values())
    print(f"  ✓ Indexed {total_cu} ClickUp records across {len(clickup_by_state)-1} states")
    
    # Step 5: Match addresses with combined scoring
    print("\n[5/6] Matching with address + facility name...")
    results = []
    match_threshold = 0.5
    stats = {'hs_state_match': 0, 'hs_fallback': 0, 'cu_state_match': 0, 'cu_fallback': 0, 
             'corp_state_match': 0, 'corp_fallback': 0}
    
    for i, (unique_addr, facility_name) in enumerate(zip(unique_addresses, facility_names), 1):
        if i % 50 == 0 or i == 1:
            print(f"  [{i}/{len(unique_addresses)}] Processing...")
        
        target_state = extract_state_from_address(unique_addr)
        
        # Find HubSpot match using combined scoring
        hs_match = None
        hs_score = 0.0
        if target_state and target_state in hubspot_by_state:
            hs_match, hs_score = find_best_match_combined(
                unique_addr, facility_name, hubspot_by_state[target_state]
            )
            if hs_match and hs_score >= match_threshold:
                stats['hs_state_match'] += 1
        
        if not hs_match or hs_score < match_threshold:
            all_hs = []
            for state, candidates in hubspot_by_state.items():
                all_hs.extend(candidates)
            hs_match, hs_score = find_best_match_combined(unique_addr, facility_name, all_hs)
            if hs_match and hs_score >= match_threshold:
                stats['hs_fallback'] += 1
        
        # Find ClickUp match using combined scoring
        cu_match = None
        cu_score = 0.0
        if target_state and target_state in clickup_by_state:
            cu_match, cu_score = find_best_match_combined(
                unique_addr, facility_name, clickup_by_state[target_state]
            )
            if cu_match and cu_score >= match_threshold:
                stats['cu_state_match'] += 1
        
        if not cu_match or cu_score < match_threshold:
            all_cu = []
            for state, candidates in clickup_by_state.items():
                all_cu.extend(candidates)
            cu_match, cu_score = find_best_match_combined(unique_addr, facility_name, all_cu)
            if cu_match and cu_score >= match_threshold:
                stats['cu_fallback'] += 1
        
        # Find Corporations list match by name only (no addresses in Corporations list)
        corp_match = None
        corp_score = 0.0
        if corporations_candidates:
            # For Corporations, we only match by facility name since there's no address
            best_corp_match = None
            best_corp_score = 0.0
            for addr_str, name_str, data in corporations_candidates:
                if USE_RAPIDFUZZ:
                    score = fuzz.token_set_ratio(facility_name, name_str) / 100.0
                else:
                    score = SequenceMatcher(None, normalize_address(facility_name), 
                                              normalize_address(name_str)).ratio()
                if score > best_corp_score:
                    best_corp_score = score
                    best_corp_match = data
            
            if best_corp_match and best_corp_score >= match_threshold:
                corp_match = best_corp_match
                corp_score = best_corp_score
                stats['corp_fallback'] += 1
        
        result = {
            'unique_address': unique_addr,
            'facility_name': facility_name,
            'target_state': target_state or '',
            'hubspot_company_name': hs_match.get('hubspot_company_name') if hs_match and hs_score >= match_threshold else '',
            'hubspot_record_id': hs_match.get('hubspot_record_id') if hs_match and hs_score >= match_threshold else '',
            'hubspot_full_address': hs_match.get('hubspot_full_address') if hs_match and hs_score >= match_threshold else '',
            'hubspot_parent_company': hs_match.get('hubspot_parent_company') if hs_match and hs_score >= match_threshold else '',
            'hubspot_match_score': round(hs_score, 3) if hs_match else 0,
            'clickup_task_name': cu_match.get('clickup_task_name') if cu_match and cu_score >= match_threshold else '',
            'clickup_task_location': cu_match.get('clickup_task_location') if cu_match and cu_score >= match_threshold else '',
            'clickup_match_score': round(cu_score, 3) if cu_match else 0,
            'clickup_task_url': cu_match.get('clickup_url') if cu_match and cu_score >= match_threshold else '',
            'corporations_task_name': corp_match.get('corporations_task_name') if corp_match and corp_score >= match_threshold else '',
            'corporations_task_location': corp_match.get('corporations_task_location') if corp_match and corp_score >= match_threshold else '',
            'corporations_task_url': corp_match.get('corporations_task_url') if corp_match and corp_score >= match_threshold else '',
            'corporations_match_score': round(corp_score, 3) if corp_match else 0,
        }
        results.append(result)
    
    print(f"  ✓ Completed matching for {len(results)} addresses")
    
    # Step 6: Save results
    print("\n[6/6] Saving results to Excel...")
    results_df = pd.DataFrame(results)
    
    column_order = [
        'unique_address',
        'facility_name',
        'target_state',
        'hubspot_company_name',
        'hubspot_record_id',
        'hubspot_full_address',
        'hubspot_parent_company',
        'hubspot_match_score',
        'clickup_task_name',
        'clickup_task_location',
        'clickup_task_url',
        'clickup_match_score',
        'corporations_task_name',
        'corporations_task_location',
        'corporations_task_url',
        'corporations_match_score',
    ]
    results_df = results_df[column_order]
    
    try:
        with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            results_df.to_excel(writer, sheet_name=output_sheet, index=False)
        print(f"  ✓ Results saved to '{output_sheet}' sheet in {excel_file}")
    except Exception as e:
        output_file = 'matched_facilities_results.xlsx'
        results_df.to_excel(output_file, sheet_name=output_sheet, index=False)
        print(f"  ✓ Results saved to new file: {output_file}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("Matching Summary (Address + Facility Name)")
    print("=" * 60)
    hs_matches = sum(1 for r in results if r['hubspot_match_score'] >= match_threshold)
    cu_matches = sum(1 for r in results if r['clickup_match_score'] >= match_threshold)
    corp_matches = sum(1 for r in results if r['corporations_match_score'] >= match_threshold)
    
    print(f"Total unique addresses: {len(results)}")
    print(f"Addresses with facility names: {sum(1 for r in results if r['facility_name'])}")
    print(f"HubSpot matches: {hs_matches} (state-filtered: {stats['hs_state_match']}, fallback: {stats['hs_fallback']})")
    print(f"ClickUp matches: {cu_matches} (state-filtered: {stats['cu_state_match']}, fallback: {stats['cu_fallback']})")
    print(f"Corporations list matches: {corp_matches} (state-filtered: {stats['corp_state_match']}, fallback: {stats['corp_fallback']})")
    print(f"Match threshold: {match_threshold}")
    print("=" * 60)


if __name__ == '__main__':
    main()
