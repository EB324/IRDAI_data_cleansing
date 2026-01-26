#!/usr/bin/env python3
"""
Indian Insurance Handbook ETL Pipeline
Extracts, transforms, and standardizes data from IRDAI Insurance Handbook workbooks
into analysis-ready outputs with standardized schemas.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================

CRORE_TO_RUPEES = 10_000_000  # 1 Crore = 10,000,000

# Canonical insurer name mappings for standardization
INSURER_CANONICAL_NAMES = {
    'life insurance corporation of india': 'LIC',
    'lic of india': 'LIC',
    'lic': 'LIC',
    'aditya birla sunlife insurance company ltd': 'ABSLI',
    'aditya birla sun life insurance company ltd': 'ABSLI',
    'aditya birla sunlife': 'ABSLI',
    'aditya birla sun life': 'ABSLI',
    'aditya birla sunlife insurance co ltd': 'ABSLI',
    'icici prudential life insurance company ltd': 'ICICI Pru Life',
    'icici prudential life insurance': 'ICICI Pru Life',
    'icici pru life': 'ICICI Pru Life',
    'sbi life insurance company ltd': 'SBI Life',
    'sbi life insurance': 'SBI Life',
    'sbi life': 'SBI Life',
    'max life insurance company ltd': 'MaxLife',
    'max life insurance': 'MaxLife',
    'maxlife insurance company ltd': 'MaxLife',
    'maxlife': 'MaxLife',
    'tata aia life insurance company ltd': 'Tata AIA',
    'tata aia life insurance': 'Tata AIA',
    'tata aia': 'Tata AIA',
    'pnb metlife india insurance company ltd': 'PNB Metlife',
    'pnb metlife india insurance': 'PNB Metlife',
    'pnb metlife': 'PNB Metlife',
    'canara hsbc obc life insurance company ltd': 'Canara HSBC',
    'canara hsbc life insurance company ltd': 'Canara HSBC',
    'canara hsbc life insurance': 'Canara HSBC',
    'canara hsbc': 'Canara HSBC',
    'hdfc life insurance company ltd': 'HDFC Life',
    'hdfc life insurance': 'HDFC Life',
    'hdfc life': 'HDFC Life',
    'kotak mahindra life insurance ltd': 'Kotak Life',
    'kotak mahindra life insurance': 'Kotak Life',
    'kotak life': 'Kotak Life',
    'bajaj allianz life insurance company ltd': 'Bajaj Allianz Life',
    'bajaj allianz life insurance': 'Bajaj Allianz Life',
    'bajaj allianz life': 'Bajaj Allianz Life',
    'bharti axa life insurance company ltd': 'Bharti AXA Life',
    'bharti axa life insurance': 'Bharti AXA Life',
    'bharti axa life': 'Bharti AXA Life',
    'exide life insurance company ltd': 'Exide Life',
    'exide life insurance': 'Exide Life',
    'exide life': 'Exide Life',
    'aviva life insurance company india ltd': 'Aviva Life',
    'aviva life insurance': 'Aviva Life',
    'aviva life': 'Aviva Life',
    'ageas federal life insurance company ltd': 'Ageas Federal Life',
    'ageas federal life insurance': 'Ageas Federal Life',
    'ageas federal life': 'Ageas Federal Life',
    'future generali india life insurance company ltd': 'Future Generali Life',
    'future generali india life insurance': 'Future Generali Life',
    'future generali life': 'Future Generali Life',
    'edelweiss tokio life insurance company ltd': 'Edelweiss Tokio Life',
    'edelweiss tokio life insurance': 'Edelweiss Tokio Life',
    'edelweiss tokio life': 'Edelweiss Tokio Life',
    'indiafirst life insurance company ltd': 'IndiaFirst Life',
    'indiafirst life insurance': 'IndiaFirst Life',
    'indiafirst life': 'IndiaFirst Life',
    'bandhan life insurance company ltd': 'Bandhan Life',
    'bandhan life insurance ltd': 'Bandhan Life',
    'bandhan life insurance': 'Bandhan Life',
    'bandhan life': 'Bandhan Life',
    'acko life insurance ltd': 'Acko Life',
    'acko life insurance': 'Acko Life',
    'acko life': 'Acko Life',
    'credit access life': 'Credit Access Life',
    'creditaccess life insurance ltd': 'Credit Access Life',
    'go digit life': 'Go Digit Life',
    'go digit life insurance': 'Go Digit Life',
    'go digit life insurance limited': 'Go Digit Life',
    'pramerica life insurance ltd': 'Pramerica Life',
    'pramerica life insurance': 'Pramerica Life',
    'pramerica life': 'Pramerica Life',
    'reliance nippon life insurance company ltd': 'Reliance Nippon Life',
    'reliance nippon life insurance': 'Reliance Nippon Life',
    'reliance nippon life': 'Reliance Nippon Life',
    'sahara india life insurance company ltd': 'Sahara India Life',
    'sahara india life insurance': 'Sahara India Life',
    'sahara india life': 'Sahara India Life',
    'shriram life insurance company ltd': 'Shriram Life',
    'shriram life insurance': 'Shriram Life',
    'shriram life': 'Shriram Life',
    'star union dai-ichi life insurance company ltd': 'Star Union Dai-ichi Life',
    'star union dai-ichi life insurance': 'Star Union Dai-ichi Life',
    'star union dai-ichi life': 'Star Union Dai-ichi Life',
    'aegon life insurance company ltd': 'Aegon Life',
    'aegon life insurance': 'Aegon Life',
    'aegon life': 'Aegon Life',
}

# Distribution channel normalization
CHANNEL_MAPPING = {
    'individual agents': 'Individual Agents',
    'corporate agents - banks': 'Corporate Agents - Banks',
    'corporate agents banks': 'Corporate Agents - Banks',
    'banks': 'Corporate Agents - Banks',
    'corporate agents - others': 'Corporate Agents - Others',
    'corporate agents others': 'Corporate Agents - Others',
    'others*': 'Corporate Agents - Others',
    'brokers': 'Brokers',
    'direct selling': 'Direct Selling',
    'mi agents': 'MI Agents',
    'common service centres': 'CSCs',
    'common service centres (cscs)': 'CSCs',
    'cscs': 'CSCs',
    'web aggregators': 'Web Aggregators',
    'imf': 'IMF',
    'online': 'Online',
    'online**': 'Online',
    'point of sales': 'POS',
    'point of sales (pos)': 'POS',
    'pos': 'POS',
    'others if any': 'Others',
    'others': 'Others',
    'referrals': 'Referrals',
}

# Product category L1/L2/L3 parsing rules
CATEGORY_MAPPINGS = {
    # L1 categories
    'linked': ('Linked', None, None),
    'non-linked': ('Non-Linked', None, None),
    'non linked': ('Non-Linked', None, None),
    'nonlinked': ('Non-Linked', None, None),
    # L2 categories (under Non-Linked)
    'participating': (None, 'Participating', None),
    'non-participating': (None, 'Non-Participating', None),
    'non participating': (None, 'Non-Participating', None),
    # L3 categories
    'life': (None, None, 'Life'),
    'annuity': (None, None, 'Annuity'),
    'general annuity': (None, None, 'Annuity'),
    'pension': (None, None, 'Pension'),
    'health': (None, None, 'Health'),
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def clean_insurer_name(name: str) -> str:
    """Clean and standardize insurer name."""
    if pd.isna(name) or not isinstance(name, str):
        return ''
    # Basic cleaning
    name = str(name).strip().lower()
    # Remove common suffixes and punctuation
    name = re.sub(r'\s+(ltd\.?|limited|pvt\.?|private|inc\.?|incorporated|company|co\.?)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def standardize_insurer(name: str, name_xwalk: Dict[str, str]) -> str:
    """Standardize insurer name using crosswalk and fuzzy matching."""
    if pd.isna(name) or not isinstance(name, str):
        return ''
    
    cleaned = clean_insurer_name(name)
    if not cleaned:
        return ''
    
    # Direct lookup
    if cleaned in INSURER_CANONICAL_NAMES:
        canonical = INSURER_CANONICAL_NAMES[cleaned]
        name_xwalk[name] = canonical
        return canonical
    
    # Fuzzy matching using difflib
    best_match = None
    best_score = 0
    for key, canonical in INSURER_CANONICAL_NAMES.items():
        score = SequenceMatcher(None, cleaned, key).ratio() * 100
        if score > best_score and score >= 92:
            best_score = score
            best_match = canonical
    
    if best_match:
        name_xwalk[name] = best_match
        return best_match
    
    # Return cleaned version with title case if no match
    result = name.strip().title()
    name_xwalk[name] = result
    return result

def parse_year(year_str: str) -> Optional[int]:
    """Parse fiscal year string to ending year."""
    if pd.isna(year_str):
        return None
    year_str = str(year_str).strip()
    
    # Pattern: "2023-24" -> 2024
    match = re.search(r'(\d{4})-(\d{2})', year_str)
    if match:
        return int(match.group(1)) + 1
    
    # Pattern: "as on 31 March 2024" -> 2024
    match = re.search(r'(?:march|Mar)\s*(\d{4})', year_str, re.IGNORECASE)
    if match:
        return int(match.group(1))
    
    # Pattern: just "2024"
    match = re.search(r'\b(20\d{2})\b', year_str)
    if match:
        return int(match.group(1))
    
    return None

def parse_category_label(label: str) -> Tuple[str, str, str]:
    """Parse category label into L1, L2, L3 components."""
    if pd.isna(label) or not isinstance(label, str):
        return ('', '', '')
    
    label_lower = label.lower().strip()
    l1, l2, l3 = '', '', ''
    
    # Check for Linked/Non-Linked
    if 'linked' in label_lower:
        if 'non-linked' in label_lower or 'non linked' in label_lower or 'nonlinked' in label_lower:
            l1 = 'Non-Linked'
        else:
            l1 = 'Linked'
    
    # Check for L3 categories
    if 'life' in label_lower and 'annuity' not in label_lower:
        l3 = 'Life'
    elif 'annuity' in label_lower or 'general annuity' in label_lower:
        l3 = 'Annuity'
    elif 'pension' in label_lower:
        l3 = 'Pension'
    elif 'health' in label_lower:
        l3 = 'Health'
    
    return (l1, l2, l3)

def normalize_channel(channel: str) -> str:
    """Normalize distribution channel name."""
    if pd.isna(channel) or not isinstance(channel, str):
        return ''
    channel_lower = channel.lower().strip()
    return CHANNEL_MAPPING.get(channel_lower, channel.strip())

def convert_crore_to_rupees(value, is_crore: bool = True) -> Optional[float]:
    """Convert value from Crore to absolute Rupees."""
    if pd.isna(value) or value == '-' or value == '':
        return None
    try:
        val = float(value)
        if is_crore:
            return val * CRORE_TO_RUPEES
        return val
    except (ValueError, TypeError):
        return None

def safe_numeric(value) -> Optional[float]:
    """Safely convert value to numeric."""
    if pd.isna(value) or value == '-' or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def is_section_header(row: pd.Series) -> bool:
    """Check if row is a section header (Public Sector, Private Sector, Total)."""
    first_val = str(row.iloc[1] if len(row) > 1 else row.iloc[0]).lower().strip()
    return first_val in ['public sector', 'private sector', 'total', 'grand total', 'industry total', 'private total', 'private sector total']

# =============================================================================
# TABLE EXTRACTION FUNCTIONS
# =============================================================================

def extract_table_2(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 2: Total Premium by Insurer and Year."""
    df = pd.read_excel(xlsx_path, sheet_name='2', header=None)
    
    # Find header row with years
    header_row = None
    for i, row in df.iterrows():
        row_str = ' '.join([str(x) for x in row.values])
        if '2014-15' in row_str or '2015-16' in row_str:
            header_row = i
            break
    
    if header_row is None:
        return pd.DataFrame()
    
    # Extract years from header
    years = []
    year_cols = []
    for col in range(2, len(df.columns)):
        val = df.iloc[header_row, col]
        year = parse_year(str(val))
        if year:
            years.append(year)
            year_cols.append(col)
    
    # Extract data rows
    records = []
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for year, col in zip(years, year_cols):
            value = convert_crore_to_rupees(row.iloc[col])
            if value is not None:
                records.append({
                    'Insurer': insurer,
                    'Year': year,
                    'L1': '',
                    'L2': '',
                    'L3': '',
                    'Individual_Group': 'Not Applicable',
                    'Distribution_Channel': '',
                    'KPI': 'Total Premium',
                    'Value': value,
                    'Source': 'Part I - Table 2'
                })
    
    return pd.DataFrame(records)

def extract_table_3(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 3: New Business Premium by Insurer and Year."""
    df = pd.read_excel(xlsx_path, sheet_name='3', header=None)
    
    # Find header row with years
    header_row = None
    for i, row in df.iterrows():
        row_str = ' '.join([str(x) for x in row.values])
        if '2014-15' in row_str or '2015-16' in row_str:
            header_row = i
            break
    
    if header_row is None:
        return pd.DataFrame()
    
    # Extract years from header
    years = []
    year_cols = []
    for col in range(2, len(df.columns)):
        val = df.iloc[header_row, col]
        year = parse_year(str(val))
        if year:
            years.append(year)
            year_cols.append(col)
    
    # Extract data rows
    records = []
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for year, col in zip(years, year_cols):
            value = convert_crore_to_rupees(row.iloc[col])
            if value is not None:
                records.append({
                    'Insurer': insurer,
                    'Year': year,
                    'L1': '',
                    'L2': '',
                    'L3': '',
                    'Individual_Group': 'Not Applicable',
                    'Distribution_Channel': '',
                    'KPI': 'New Business Premium',
                    'Value': value,
                    'Source': 'Part I - Table 3'
                })
    
    return pd.DataFrame(records)

def extract_table_6(xlsx_path: str, name_xwalk: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract Table 6: State-wise Individual New Business."""
    df = pd.read_excel(xlsx_path, sheet_name='6', header=None)
    
    state_records = []
    
    insurer_row = 2
    year_row = 3
    metric_row = 4
    data_start = 5
    
    # Exclusion list for aggregate columns
    excluded_insurers = ['grand total', 'private total', 'private sector total', 'public sector total', 'total', 'industry total']
    
    # Parse column structure - track BOTH current_insurer AND current_year
    col_info = []
    
    current_insurer = None
    current_year = None
    
    for col in range(2, len(df.columns)):
        # Check for insurer name
        insurer_val = df.iloc[insurer_row, col] if insurer_row < len(df) else None
        if pd.notna(insurer_val) and str(insurer_val).strip():
            insurer_raw = str(insurer_val).strip()
            if insurer_raw.lower() not in excluded_insurers:
                current_insurer = insurer_raw
            else:
                current_insurer = None  # Reset to skip aggregate columns
        
        # Check for year - only update if present (carry forward otherwise)
        year_val = df.iloc[year_row, col] if year_row < len(df) else None
        if pd.notna(year_val) and str(year_val).strip():
            parsed_year = parse_year(str(year_val))
            if parsed_year:
                current_year = parsed_year
        
        # Get metric type
        metric_val = df.iloc[metric_row, col] if metric_row < len(df) else None
        if pd.notna(metric_val) and current_insurer and current_year:
            metric_str = str(metric_val).strip().lower()
            if 'polic' in metric_str:
                col_info.append((col, current_insurer, current_year, 'New Business Policy'))
            elif 'premium' in metric_str:
                col_info.append((col, current_insurer, current_year, 'New Business Premium'))
    
    # Extract data rows
    for i in range(data_start, len(df)):
        row = df.iloc[i]
        state = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(state) or not isinstance(state, str):
            continue
        state = state.strip()
        if not state or state.lower() in ['total', 'grand total', 'all india', 's.no.', 'private total', 'private sector total', 'public sector total']:
            continue
        
        for col, insurer_raw, year, kpi in col_info:
            if col < len(row):
                insurer = standardize_insurer(insurer_raw, name_xwalk)
                if not insurer:
                    continue
                
                raw_value = row.iloc[col]
                
                if kpi == 'New Business Premium':
                    value = convert_crore_to_rupees(raw_value)
                else:
                    value = safe_numeric(raw_value)
                    if value is not None:
                        value = int(value) if value == int(value) else value
                
                if value is not None:
                    state_records.append({
                        'State': state,
                        'Insurer': insurer,
                        'Year': year,
                        'Individual_Group': 'Individual',
                        'KPI': kpi,
                        'Value': value,
                        'Source': 'Part I - Table 6'
                    })
    
    state_df = pd.DataFrame(state_records)
    
    # Aggregate for facts table
    if len(state_df) > 0:
        agg = state_df.groupby(['Insurer', 'Year', 'Individual_Group', 'KPI', 'Source'])['Value'].sum().reset_index()
        agg['L1'] = ''
        agg['L2'] = ''
        agg['L3'] = ''
        agg['Distribution_Channel'] = ''
        agg_df = agg[['Insurer', 'Year', 'L1', 'L2', 'L3', 'Individual_Group', 'Distribution_Channel', 'KPI', 'Value', 'Source']]
    else:
        agg_df = pd.DataFrame()
    
    return agg_df, state_df

def extract_table_8(xlsx_path: str, name_xwalk: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract Table 8: State-wise Group Business."""
    df = pd.read_excel(xlsx_path, sheet_name='8', header=None)
    
    state_records = []
    
    # Structure:
    # Row 1: Insurer names
    # Row 2: Years (2022-23, 2023-24)
    # Row 3: Metrics (No. of Schemes, Lives Covered, Premium)
    # Row 4+: Data rows with State in col 1
    
    insurer_row = 1
    year_row = 2
    metric_row = 3
    data_start = 4
    
    # Exclusion list for aggregate columns
    excluded_insurers = ['grand total', 'private total', 'private sector total', 'public sector total', 'total', 'industry total']
    
    # Parse column structure
    col_info = []  # (col, insurer, year, metric_type)
    
    current_insurer = None
    current_year = None
    for col in range(2, len(df.columns)):
        # Check for insurer name
        insurer_val = df.iloc[insurer_row, col] if insurer_row < len(df) else None
        if pd.notna(insurer_val) and str(insurer_val).strip():
            insurer_raw = str(insurer_val).strip()
            if insurer_raw.lower() not in excluded_insurers:
                current_insurer = insurer_raw
            else:
                current_insurer = None  # Reset to skip aggregate columns
        
        # Get year
        year_val = df.iloc[year_row, col] if year_row < len(df) else None
        if pd.notna(year_val):
            year = parse_year(str(year_val))
            if year:
                current_year = year
        
        # Get metric type
        metric_val = df.iloc[metric_row, col] if metric_row < len(df) else None
        if pd.notna(metric_val) and current_insurer and current_year:
            metric_str = str(metric_val).strip().lower()
            if 'premium' in metric_str:
                col_info.append((col, current_insurer, current_year, 'New Business Premium'))
            # We only extract premium for Group business as per spec
    
    # Extract data rows
    for i in range(data_start, len(df)):
        row = df.iloc[i]
        state = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(state) or not isinstance(state, str):
            continue
        state = state.strip()
        if not state or state.lower() in ['total', 'grand total', 'all india', 's.no.', 'private total', 'private sector total', 'public sector total']:
            continue
        
        for col, insurer_raw, year, kpi in col_info:
            if col < len(row):
                insurer = standardize_insurer(insurer_raw, name_xwalk)
                if not insurer:
                    continue
                
                value = convert_crore_to_rupees(row.iloc[col])
                
                if value is not None:
                    state_records.append({
                        'State': state,
                        'Insurer': insurer,
                        'Year': year,
                        'Individual_Group': 'Group',
                        'KPI': kpi,
                        'Value': value,
                        'Source': 'Part I - Table 8'
                    })
    
    state_df = pd.DataFrame(state_records)
    
    # Aggregate for facts table
    if len(state_df) > 0:
        agg = state_df.groupby(['Insurer', 'Year', 'Individual_Group', 'KPI', 'Source'])['Value'].sum().reset_index()
        agg['L1'] = ''
        agg['L2'] = ''
        agg['L3'] = ''
        agg['Distribution_Channel'] = ''
        agg_df = agg[['Insurer', 'Year', 'L1', 'L2', 'L3', 'Individual_Group', 'Distribution_Channel', 'KPI', 'Value', 'Source']]
    else:
        agg_df = pd.DataFrame()
    
    return agg_df, state_df

def extract_table_10(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 10: Individual Business in Force (Policies) by product category."""
    df = pd.read_excel(xlsx_path, sheet_name='10', header=None)
    
    records = []
    
    # Parse insurer/year columns from header rows
    insurer_row = 2
    year_row = 3
    
    # Exclusion list for aggregate columns
    excluded_insurers = ['particulars', 'nan', 'grand total', 'private total', 'private sector total', 'public sector total', 'total', 'industry total']
    
    col_info = []
    current_insurer = None
    
    for col in range(1, len(df.columns)):
        insurer_val = df.iloc[insurer_row, col]
        if pd.notna(insurer_val) and str(insurer_val).strip():
            insurer_raw = str(insurer_val).strip()
            if insurer_raw.lower() not in excluded_insurers:
                current_insurer = insurer_raw
            else:
                current_insurer = None  # Reset to skip aggregate columns
        
        year_val = df.iloc[year_row, col]
        year = parse_year(str(year_val)) if pd.notna(year_val) else None
        
        if current_insurer and year:
            col_info.append((col, current_insurer, year))
    
    # Category mapping: category header row -> (L1, L2, L3)
    # Categories are identified by their header rows, data is in "Business in force at end" rows
    category_map = {
        'non linked life business': ('Non-Linked', '', 'Life'),
        'non linked -general annuity business': ('Non-Linked', '', 'Annuity'),
        'non linked - pension business': ('Non-Linked', '', 'Pension'),
        'non linked health business': ('Non-Linked', '', 'Health'),
        'linked business - life business': ('Linked', '', 'Life'),
        'linked general annuity business': ('Linked', '', 'Annuity'),
        'linked pension business': ('Linked', '', 'Pension'),
        'linked health business': ('Linked', '', 'Health'),
        'non-linked vip-life business': ('Non-Linked', 'VIP', 'Life'),
        'non-linked vip-general annuity business': ('Non-Linked', 'VIP', 'Annuity'),
        'non-linked vip-pension business': ('Non-Linked', 'VIP', 'Pension'),
        'non-linked vip-health business': ('Non-Linked', 'VIP', 'Health'),
        'linked vip-life business': ('Linked', 'VIP', 'Life'),
        'linked vip-general annuity business': ('Linked', 'VIP', 'Annuity'),
        'linked vip-pension business': ('Linked', 'VIP', 'Pension'),
        'linked vip-health business': ('Linked', 'VIP', 'Health'),
    }
    
    # Find category data rows - scan for "Business in force at end of the financial year" rows
    current_category = None
    for i in range(4, len(df)):
        row_label = df.iloc[i, 0]
        if pd.isna(row_label):
            continue
        row_label_str = str(row_label).strip().lower()
        
        # Check if this is a category header
        for cat_key, (l1, l2, l3) in category_map.items():
            if cat_key in row_label_str:
                current_category = (l1, l2, l3)
                break
        
        # Skip Grand Total and Private Sector Total rows
        if 'grand total' in row_label_str or 'private sector total' in row_label_str or 'a+b+c+d' in row_label_str:
            continue
        
        # Check if this is a "Business in force at end" row with a letter designation (A, B, C, etc.)
        if current_category and 'business in force at end of the financial year' in row_label_str:
            # Check for letter designation like (A), (B), etc.
            import re
            letter_match = re.search(r'\(\s*([A-P])\s*\)', row_label_str, re.IGNORECASE)
            if letter_match:
                l1, l2, l3 = current_category
                
                for col, insurer_raw, year in col_info:
                    insurer = standardize_insurer(insurer_raw, name_xwalk)
                    if not insurer:
                        continue
                    
                    # Values are in '000, convert to absolute
                    value = safe_numeric(df.iloc[i, col])
                    if value is not None:
                        value = value * 1000  # Convert from '000
                        records.append({
                            'Insurer': insurer,
                            'Year': year,
                            'L1': l1,
                            'L2': l2,
                            'L3': l3,
                            'Individual_Group': 'Individual',
                            'Distribution_Channel': '',
                            'KPI': 'Total Policy (Year-End)',
                            'Value': int(value) if value == int(value) else value,
                            'Source': 'Part I - Table 10'
                        })
    
    return pd.DataFrame(records)

def extract_table_11(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 11: Sum Assured of Policies in Force by product category."""
    df = pd.read_excel(xlsx_path, sheet_name='11 ', header=None)
    
    records = []
    
    # Parse insurer/year columns from header rows
    insurer_row = 2
    year_row = 3
    
    # Exclusion list for aggregate columns
    excluded_insurers = ['particulars', 'nan', 'grand total', 'private total', 'private sector total', 'public sector total', 'total', 'industry total']
    
    col_info = []
    current_insurer = None
    
    for col in range(1, len(df.columns)):
        insurer_val = df.iloc[insurer_row, col]
        if pd.notna(insurer_val) and str(insurer_val).strip():
            insurer_raw = str(insurer_val).strip()
            if insurer_raw.lower() not in excluded_insurers:
                current_insurer = insurer_raw
            else:
                current_insurer = None  # Reset to skip aggregate columns
        
        year_val = df.iloc[year_row, col]
        year = parse_year(str(year_val)) if pd.notna(year_val) else None
        
        if current_insurer and year:
            col_info.append((col, current_insurer, year))
    
    # Category mapping: category header row -> (L1, L2, L3)
    category_map = {
        'non linked life business': ('Non-Linked', '', 'Life'),
        'non linked -general annuity business': ('Non-Linked', '', 'Annuity'),
        'non linked - pension business': ('Non-Linked', '', 'Pension'),
        'non linked health business': ('Non-Linked', '', 'Health'),
        'linked business - life business': ('Linked', '', 'Life'),
        'linked general annuity business': ('Linked', '', 'Annuity'),
        'linked pension business': ('Linked', '', 'Pension'),
        'linked health business': ('Linked', '', 'Health'),
        'non-linked vip-life business': ('Non-Linked', 'VIP', 'Life'),
        'non-linked vip-general annuity business': ('Non-Linked', 'VIP', 'Annuity'),
        'non-linked vip-pension business': ('Non-Linked', 'VIP', 'Pension'),
        'non-linked vip-health business': ('Non-Linked', 'VIP', 'Health'),
        'linked vip-life business': ('Linked', 'VIP', 'Life'),
        'linked vip-general annuity business': ('Linked', 'VIP', 'Annuity'),
        'linked vip-pension business': ('Linked', 'VIP', 'Pension'),
        'linked vip-health business': ('Linked', 'VIP', 'Health'),
    }
    
    # Find category data rows
    current_category = None
    for i in range(4, len(df)):
        row_label = df.iloc[i, 0]
        if pd.isna(row_label):
            continue
        row_label_str = str(row_label).strip().lower()
        
        # Check if this is a category header
        for cat_key, (l1, l2, l3) in category_map.items():
            if cat_key in row_label_str:
                current_category = (l1, l2, l3)
                break
        
        # Skip Grand Total and Private Sector Total rows
        if 'grand total' in row_label_str or 'private sector total' in row_label_str or 'a + b + c + d' in row_label_str:
            continue
        
        # Check if this is a "Business in force at end" row with a letter designation (A, B, C, etc.)
        if current_category and 'business in force at end of the financial year' in row_label_str:
            import re
            letter_match = re.search(r'\(\s*([A-P])\s*\)', row_label_str, re.IGNORECASE)
            if letter_match:
                l1, l2, l3 = current_category
                
                for col, insurer_raw, year in col_info:
                    insurer = standardize_insurer(insurer_raw, name_xwalk)
                    if not insurer:
                        continue
                    
                    # Values are in Crore, convert to absolute Rupees
                    value = convert_crore_to_rupees(df.iloc[i, col])
                    if value is not None:
                        records.append({
                            'Insurer': insurer,
                            'Year': year,
                            'L1': l1,
                            'L2': l2,
                            'L3': l3,
                            'Individual_Group': 'Individual',
                            'Distribution_Channel': '',
                            'KPI': 'Sum Assured (Year-End)',
                            'Value': value,
                            'Source': 'Part I - Table 11'
                        })
    
    return pd.DataFrame(records)

def extract_table_12(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 12: Linked and Non-Linked Premium breakdown."""
    df = pd.read_excel(xlsx_path, sheet_name='12', header=None)
    
    records = []
    
    # Structure:
    # Row 2: Main categories (A. LINKED, B. NON-LINKED, C. TOTAL)
    # Row 3: Sub-categories (First Year, Single, New Business, Renewal, Total)
    # Row 4: Years
    # Row 5+: Data rows
    
    # Column mapping:
    # A. LINKED PREMIUM e.Total: cols 42-51 (years 2014-15 to 2023-24)
    # B. NON-LINKED PREMIUM e.Total: cols 92-101 (years 2014-15 to 2023-24)
    
    year_row = 4
    data_start = 6  # Skip header rows and "Public Sector" label
    
    # Define column groups for Total Premium
    column_groups = [
        ('Linked', range(42, 52)),      # e.Total for Linked
        ('Non-Linked', range(92, 102)), # e.Total for Non-Linked
    ]
    
    # Get years for each group
    for l1_category, col_range in column_groups:
        year_cols = []
        for col in col_range:
            year_val = df.iloc[year_row, col] if year_row < len(df) and col < len(df.columns) else None
            year = parse_year(str(year_val)) if pd.notna(year_val) else None
            if year:
                year_cols.append((col, year))
        
        # Extract data rows
        for i in range(data_start, len(df)):
            row = df.iloc[i]
            insurer_raw = row.iloc[1] if len(row) > 1 else None
            
            if pd.isna(insurer_raw) or is_section_header(row):
                continue
            
            insurer = standardize_insurer(str(insurer_raw), name_xwalk)
            if not insurer:
                continue
            
            for col, year in year_cols:
                if col < len(row):
                    value = convert_crore_to_rupees(row.iloc[col])
                    if value is not None:
                        records.append({
                            'Insurer': insurer,
                            'Year': year,
                            'L1': l1_category,
                            'L2': '',
                            'L3': '',
                            'Individual_Group': 'Not Applicable',
                            'Distribution_Channel': '',
                            'KPI': 'Total Premium',
                            'Value': value,
                            'Source': 'Part I - Table 12'
                        })
    
    return pd.DataFrame(records)

def extract_table_21(xlsx_path: str, name_xwalk: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract Table 21: Assets Under Management."""
    df = pd.read_excel(xlsx_path, sheet_name='21', header=None)
    
    records = []
    table_21_records = []
    
    # Find header structure
    # Typically: S.No. | Insurer | Fund Type (Years) | ...
    
    header_row = None
    for i, row in df.iterrows():
        row_str = ' '.join([str(x) for x in row.values])
        if '2021' in row_str or '2022' in row_str:
            header_row = i
            break
    
    if header_row is None:
        return pd.DataFrame(), pd.DataFrame()
    
    # Parse column structure - check both row 3 and row 4 for fund types
    year_cols = []
    
    prev_fund = None
    for col in range(2, len(df.columns)):
        # Check row 3 (main category) and row 4 (sub-category) for fund type
        fund_val_row3 = df.iloc[3, col] if col < df.shape[1] else None
        fund_val_row4 = df.iloc[4, col] if col < df.shape[1] else None
        year_val = df.iloc[header_row, col]
        
        # Update prev_fund from row 3 first (main categories like "Grand Total (All Funds)")
        if pd.notna(fund_val_row3) and str(fund_val_row3).strip():
            prev_fund = str(fund_val_row3).strip()
        # Then check row 4 (sub-categories like "Total (Life Fund)")
        elif pd.notna(fund_val_row4) and str(fund_val_row4).strip():
            prev_fund = str(fund_val_row4).strip()
        
        year = parse_year(str(year_val))
        if year:
            year_cols.append((col, prev_fund if prev_fund else 'Total', year))
    
    # Extract data
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for col, fund_type, year in year_cols:
            value = convert_crore_to_rupees(row.iloc[col])
            if value is not None:
                # Facts table record - ONLY Grand Total (All Funds), not sub-totals
                if 'grand total' in fund_type.lower():
                    records.append({
                        'Insurer': insurer,
                        'Year': year,
                        'L1': '',
                        'L2': '',
                        'L3': '',
                        'Individual_Group': 'Not Applicable',
                        'Distribution_Channel': '',
                        'KPI': 'Assets Under Management',
                        'Value': value,
                        'Source': 'Part I - Table 21'
                    })
                
                # Table 21 detail record (keep all fund types for reference)
                table_21_records.append({
                    'Insurer': insurer,
                    'Year': year,
                    'Fund_Type': fund_type,
                    'AUM': value,
                    'Source': 'Part I - Table 21'
                })
    
    return pd.DataFrame(records), pd.DataFrame(table_21_records)

def extract_table_23(xlsx_path: str, name_xwalk: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract Table 23: Solvency Ratio."""
    df = pd.read_excel(xlsx_path, sheet_name='23', header=None)
    
    records = []
    table_23_records = []
    
    # Find header row with dates
    header_row = None
    for i, row in df.iterrows():
        row_str = ' '.join([str(x) for x in row.values])
        if 'march' in row_str.lower() or 'Mar' in row_str:
            header_row = i
            break
    
    if header_row is None:
        return pd.DataFrame(), pd.DataFrame()
    
    # Parse date columns
    date_cols = []
    for col in range(2, len(df.columns)):
        date_val = df.iloc[header_row, col]
        year = parse_year(str(date_val))
        if year:
            date_cols.append((col, year, str(date_val).strip()))
    
    # Extract data
    for i in range(header_row + 1, len(df)):
        row = df.iloc[i]
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for col, year, date_str in date_cols:
            value = safe_numeric(row.iloc[col])
            if value is not None:
                # Facts table (using March values for year-end)
                if 'march' in date_str.lower() or 'Mar' in date_str:
                    records.append({
                        'Insurer': insurer,
                        'Year': year,
                        'L1': '',
                        'L2': '',
                        'L3': '',
                        'Individual_Group': 'Not Applicable',
                        'Distribution_Channel': '',
                        'KPI': 'Solvency Ratio',
                        'Value': value,
                        'Source': 'Part I - Table 23'
                    })
                
                # Table 23 detail record
                table_23_records.append({
                    'Insurer': insurer,
                    'Period': date_str,
                    'Year': year,
                    'Solvency_Ratio': value,
                    'Source': 'Part I - Table 23'
                })
    
    return pd.DataFrame(records), pd.DataFrame(table_23_records)

def extract_table_28(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 28: Persistency Ratios (based on number of policies)."""
    df = pd.read_excel(xlsx_path, sheet_name='28', header=None)
    
    records = []
    
    # Structure: S.No. | Insurer | Year1 (13M, 25M, 37M, 49M, 61M) | Year2 (13M, 25M, 37M, 49M, 61M) | ...
    
    # Find year row and tenor row
    year_row = 3
    tenor_row = 4
    
    # Parse columns
    col_mapping = []
    current_year = None
    
    for col in range(2, len(df.columns)):
        year_val = df.iloc[year_row, col]
        tenor_val = df.iloc[tenor_row, col]
        
        year = parse_year(str(year_val))
        if year:
            current_year = year
        
        if current_year and pd.notna(tenor_val):
            tenor_str = str(tenor_val).strip()
            # Parse tenor: "13*" -> "13M"
            match = re.match(r'(\d+)\*?', tenor_str)
            if match:
                tenor = f"{match.group(1)}M"
                col_mapping.append((col, current_year, tenor))
    
    # Extract data
    for i in range(tenor_row + 1, len(df)):
        row = df.iloc[i]
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for col, year, tenor in col_mapping:
            value = safe_numeric(row.iloc[col])
            if value is not None:
                # Ensure persistency is 0-100 scale
                if value > 100:
                    value = value / 100  # Some values might be already as percentage
                
                records.append({
                    'Insurer': insurer,
                    'Year': year,
                    'L1': '',
                    'L2': '',
                    'L3': '',
                    'Individual_Group': 'Individual',
                    'Distribution_Channel': '',
                    'KPI': f'Persistency ({tenor}, Policy)',
                    'Value': value,
                    'Source': 'Part I - Table 28'
                })
    
    return pd.DataFrame(records)

def extract_table_29(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 29: State-wise Distribution of Offices of Life Insurers."""
    df = pd.read_excel(xlsx_path, sheet_name='29', header=None)
    
    records = []
    
    # Structure: S.No. | State | LIC (years) | Insurer2 (years) | ...
    # Row 1: Insurer names
    # Row 2: Years (2014-15 to 2023-24)
    # Rows 3-38: State data
    
    header_row = df.iloc[1]  # Insurer names
    year_row = df.iloc[2]    # Years
    
    # Find column positions for each insurer
    insurer_cols = {}
    current_insurer = None
    current_start = None
    
    for col_idx in range(2, df.shape[1]):
        val = header_row.iloc[col_idx]
        if pd.notna(val) and str(val).strip():
            raw_name = str(val).strip()
            # Skip aggregate columns
            if any(skip in raw_name.lower() for skip in ['total', 'sector', 'grand']):
                if current_insurer:
                    insurer_cols[current_insurer] = (current_start, col_idx - 1)
                    current_insurer = None
                continue
            
            insurer_name = standardize_insurer(raw_name, name_xwalk)
            if insurer_name:
                if current_insurer:
                    insurer_cols[current_insurer] = (current_start, col_idx - 1)
                current_insurer = insurer_name
                current_start = col_idx
    
    # Handle last insurer
    if current_insurer:
        insurer_cols[current_insurer] = (current_start, df.shape[1] - 1)
    
    # Extract data for each insurer and state
    for row_idx in range(3, 39):  # States are in rows 3-38
        state_name = df.iloc[row_idx, 1]
        if pd.isna(state_name):
            continue
        state_name = str(state_name).strip()
        if state_name.lower() in ['total', 'grand total', 'nan', '']:
            continue
        
        for insurer, (start_col, end_col) in insurer_cols.items():
            for col_idx in range(start_col, end_col + 1):
                year_val = year_row.iloc[col_idx]
                year = parse_year(year_val)
                if year and 2014 <= year <= 2025:
                    value = df.iloc[row_idx, col_idx]
                    if pd.notna(value):
                        try:
                            # Handle '-' or '--' as 0
                            if str(value).strip() in ['-', '--', '']:
                                num_value = 0
                            else:
                                num_value = int(float(value))
                            
                            records.append({
                                'State': state_name,
                                'Insurer': insurer,
                                'Year': year,
                                'Individual_Group': 'Not Applicable',
                                'KPI': 'Number of Offices',
                                'Value': num_value,
                                'Source': 'Part I - Table 29'
                            })
                        except (ValueError, TypeError):
                            pass
    
    return pd.DataFrame(records)

def extract_table_100(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 100: Individual New Business by Distribution Channel (2023-24)."""
    df = pd.read_excel(xlsx_path, sheet_name='100', header=None)
    
    records = []
    
    # Hardcoded column mapping based on actual table structure
    CHANNEL_MAP = {
        2: ('Individual Agents', 'Policies'),
        3: ('Individual Agents', 'Premium'),
        4: ('Corporate Agents - Banks', 'Policies'),
        5: ('Corporate Agents - Banks', 'Premium'),
        6: ('Corporate Agents - Others', 'Policies'),
        7: ('Corporate Agents - Others', 'Premium'),
        8: ('Brokers', 'Policies'),
        9: ('Brokers', 'Premium'),
        10: ('Direct Selling', 'Policies'),
        11: ('Direct Selling', 'Premium'),
        12: ('MI Agents', 'Policies'),
        13: ('MI Agents', 'Premium'),
        14: ('CSCs', 'Policies'),
        15: ('CSCs', 'Premium'),
        16: ('Web Aggregators', 'Policies'),
        17: ('Web Aggregators', 'Premium'),
        18: ('IMF', 'Policies'),
        19: ('IMF', 'Premium'),
        20: ('Online', 'Policies'),
        21: ('Online', 'Premium'),
        22: ('POS', 'Policies'),
        23: ('POS', 'Premium'),
        24: ('Others', 'Policies'),
        25: ('Others', 'Premium'),
        # Skip 26-27: Total Individual New Business
    }
    
    # Extract data starting from row 5
    data_start = 5
    for i in range(data_start, len(df)):
        row = df.iloc[i]
        s_no = row.iloc[0] if len(row) > 0 else None
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        # Skip percentage rows and headers
        if pd.isna(s_no) or not isinstance(s_no, (int, float)):
            continue
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for col, (channel, metric_type) in CHANNEL_MAP.items():
            if col < len(row):
                raw_value = row.iloc[col]
                
                if metric_type == 'Premium':
                    value = convert_crore_to_rupees(raw_value)
                    kpi = 'New Business Premium'
                else:
                    value = safe_numeric(raw_value)
                    if value is not None:
                        value = int(value) if value == int(value) else value
                    kpi = 'New Business Policy'
                
                if value is not None:
                    records.append({
                        'Insurer': insurer,
                        'Year': 2024,
                        'L1': '',
                        'L2': '',
                        'L3': '',
                        'Individual_Group': 'Individual',
                        'Distribution_Channel': channel,
                        'KPI': kpi,
                        'Value': value,
                        'Source': 'Part V - Table 100'
                    })
    
    return pd.DataFrame(records)

def extract_table_102(xlsx_path: str, name_xwalk: Dict[str, str]) -> pd.DataFrame:
    """Extract Table 102: Group New Business by Distribution Channel (2023-24)."""
    df = pd.read_excel(xlsx_path, sheet_name='102', header=None)
    
    records = []
    
    # Hardcoded column mapping based on actual table structure
    # Each channel has: Schemes, Premium, Lives covered (3 columns each)
    CHANNEL_MAP = {
        # Individual Agents: cols 2-4
        3: ('Individual Agents', 'Premium'),
        # Corporate Agents - Banks: cols 5-7
        6: ('Corporate Agents - Banks', 'Premium'),
        # Corporate Agents - Others: cols 8-10
        9: ('Corporate Agents - Others', 'Premium'),
        # Brokers: cols 11-13
        12: ('Brokers', 'Premium'),
        # Direct Selling: cols 14-16
        15: ('Direct Selling', 'Premium'),
        # MI Agents: cols 17-19
        18: ('MI Agents', 'Premium'),
        # CSCs: cols 20-22
        21: ('CSCs', 'Premium'),
        # Web Aggregators: cols 23-25
        24: ('Web Aggregators', 'Premium'),
        # IMF: cols 26-28
        27: ('IMF', 'Premium'),
        # Online: cols 29-31
        30: ('Online', 'Premium'),
        # POS: cols 32-34
        33: ('POS', 'Premium'),
        # Others: cols 35-37
        36: ('Others', 'Premium'),
        # Skip 38-40: Total Group New Business
    }
    
    # Extract data starting from row 5
    data_start = 5
    for i in range(data_start, len(df)):
        row = df.iloc[i]
        s_no = row.iloc[0] if len(row) > 0 else None
        insurer_raw = row.iloc[1] if len(row) > 1 else None
        
        if pd.isna(s_no) or not isinstance(s_no, (int, float)):
            continue
        
        if pd.isna(insurer_raw) or is_section_header(row):
            continue
        
        insurer = standardize_insurer(str(insurer_raw), name_xwalk)
        if not insurer:
            continue
        
        for col, (channel, metric_type) in CHANNEL_MAP.items():
            if col < len(row):
                raw_value = row.iloc[col]
                value = convert_crore_to_rupees(raw_value)
                kpi = 'New Business Premium'
                
                if value is not None:
                    records.append({
                        'Insurer': insurer,
                        'Year': 2024,
                        'L1': '',
                        'L2': '',
                        'L3': '',
                        'Individual_Group': 'Group',
                        'Distribution_Channel': channel,
                        'KPI': kpi,
                        'Value': value,
                        'Source': 'Part V - Table 102'
                    })
    
    return pd.DataFrame(records)

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_facts_table(df: pd.DataFrame, qa_logs: List[Dict]) -> None:
    """Validate facts table data quality."""
    
    # Check for required columns
    required_cols = ['Insurer', 'Year', 'L1', 'L2', 'L3', 'Individual_Group', 
                     'Distribution_Channel', 'KPI', 'Value', 'Source']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        qa_logs.append({
            'Check': 'Required Columns',
            'Status': 'FAIL',
            'Details': f'Missing columns: {missing_cols}'
        })
    else:
        qa_logs.append({
            'Check': 'Required Columns',
            'Status': 'PASS',
            'Details': 'All required columns present'
        })
    
    # Check L1 values
    valid_l1 = {'', 'Linked', 'Non-Linked'}
    invalid_l1 = set(df['L1'].dropna().unique()) - valid_l1
    if invalid_l1:
        qa_logs.append({
            'Check': 'L1 Values',
            'Status': 'WARNING',
            'Details': f'Invalid L1 values: {invalid_l1}'
        })
    else:
        qa_logs.append({
            'Check': 'L1 Values',
            'Status': 'PASS',
            'Details': 'All L1 values valid'
        })
    
    # Check Individual/Group values
    valid_ig = {'Individual', 'Group', 'Not Applicable'}
    invalid_ig = set(df['Individual_Group'].dropna().unique()) - valid_ig
    if invalid_ig:
        qa_logs.append({
            'Check': 'Individual/Group Values',
            'Status': 'WARNING',
            'Details': f'Invalid values: {invalid_ig}'
        })
    else:
        qa_logs.append({
            'Check': 'Individual/Group Values',
            'Status': 'PASS',
            'Details': 'All Individual/Group values valid'
        })
    
    # Check Persistency values (0-100 range)
    persist_df = df[df['KPI'].str.contains('Persistency', na=False)]
    if len(persist_df) > 0:
        out_of_range = persist_df[(persist_df['Value'] < 0) | (persist_df['Value'] > 100)]
        if len(out_of_range) > 0:
            qa_logs.append({
                'Check': 'Persistency Range',
                'Status': 'WARNING',
                'Details': f'{len(out_of_range)} persistency values out of 0-100 range'
            })
        else:
            qa_logs.append({
                'Check': 'Persistency Range',
                'Status': 'PASS',
                'Details': 'All persistency values in 0-100 range'
            })
    
    # Check for null values
    null_counts = df.isnull().sum()
    value_nulls = null_counts.get('Value', 0)
    if value_nulls > 0:
        qa_logs.append({
            'Check': 'Null Values',
            'Status': 'WARNING',
            'Details': f'{value_nulls} null values in Value column'
        })
    else:
        qa_logs.append({
            'Check': 'Null Values',
            'Status': 'PASS',
            'Details': 'No null values in Value column'
        })
    
    # Record counts
    qa_logs.append({
        'Check': 'Record Count',
        'Status': 'INFO',
        'Details': f'Total records: {len(df)}'
    })
    
    qa_logs.append({
        'Check': 'Unique Insurers',
        'Status': 'INFO',
        'Details': f'Count: {df["Insurer"].nunique()}'
    })
    
    qa_logs.append({
        'Check': 'Year Range',
        'Status': 'INFO',
        'Details': f'{df["Year"].min()} - {df["Year"].max()}'
    })
    
    qa_logs.append({
        'Check': 'KPIs',
        'Status': 'INFO',
        'Details': ', '.join(df['KPI'].unique())
    })

# =============================================================================
# MAIN ETL FUNCTION
# =============================================================================

def run_etl(part1_path: str, part5_path: str, output_dir: str) -> Dict[str, pd.DataFrame]:
    """Run the complete ETL pipeline."""
    
    print("Starting ETL Pipeline...")
    
    # Initialize name crosswalk tracker
    name_xwalk = {}
    qa_logs = []
    
    # Collect all facts records
    all_facts = []
    state_breakdown_records = []
    table_21_records = []
    table_23_records = []
    
    # Part I extractions
    print("Extracting Part I tables...")
    
    # Table 2: Total Premium
    print("  - Table 2: Total Premium")
    df2 = extract_table_2(part1_path, name_xwalk)
    if len(df2) > 0:
        all_facts.append(df2)
        qa_logs.append({'Check': 'Table 2 Extraction', 'Status': 'PASS', 'Details': f'{len(df2)} records'})
    
    # Table 3: New Business Premium
    print("  - Table 3: New Business Premium")
    df3 = extract_table_3(part1_path, name_xwalk)
    if len(df3) > 0:
        all_facts.append(df3)
        qa_logs.append({'Check': 'Table 3 Extraction', 'Status': 'PASS', 'Details': f'{len(df3)} records'})
    
    # Table 6: State-wise Individual
    print("  - Table 6: State-wise Individual")
    df6_facts, df6_state = extract_table_6(part1_path, name_xwalk)
    if len(df6_facts) > 0:
        all_facts.append(df6_facts)
    if len(df6_state) > 0:
        state_breakdown_records.append(df6_state)
        qa_logs.append({'Check': 'Table 6 Extraction', 'Status': 'PASS', 'Details': f'{len(df6_state)} state records'})
    
    # Table 8: State-wise Group
    print("  - Table 8: State-wise Group")
    df8_facts, df8_state = extract_table_8(part1_path, name_xwalk)
    if len(df8_facts) > 0:
        all_facts.append(df8_facts)
    if len(df8_state) > 0:
        state_breakdown_records.append(df8_state)
        qa_logs.append({'Check': 'Table 8 Extraction', 'Status': 'PASS', 'Details': f'{len(df8_state)} state records'})
    
    # Table 10: Policies in Force
    print("  - Table 10: Policies in Force")
    df10 = extract_table_10(part1_path, name_xwalk)
    if len(df10) > 0:
        all_facts.append(df10)
        qa_logs.append({'Check': 'Table 10 Extraction', 'Status': 'PASS', 'Details': f'{len(df10)} records'})
    
    # Table 11: Sum Assured
    print("  - Table 11: Sum Assured")
    df11 = extract_table_11(part1_path, name_xwalk)
    if len(df11) > 0:
        all_facts.append(df11)
        qa_logs.append({'Check': 'Table 11 Extraction', 'Status': 'PASS', 'Details': f'{len(df11)} records'})
    
    # Table 12: Linked/Non-Linked Premium
    print("  - Table 12: Linked/Non-Linked Premium")
    df12 = extract_table_12(part1_path, name_xwalk)
    if len(df12) > 0:
        all_facts.append(df12)
        qa_logs.append({'Check': 'Table 12 Extraction', 'Status': 'PASS', 'Details': f'{len(df12)} records'})
    
    # Table 21: AUM
    print("  - Table 21: Assets Under Management")
    df21_facts, df21_detail = extract_table_21(part1_path, name_xwalk)
    if len(df21_facts) > 0:
        all_facts.append(df21_facts)
    if len(df21_detail) > 0:
        table_21_records.append(df21_detail)
        qa_logs.append({'Check': 'Table 21 Extraction', 'Status': 'PASS', 'Details': f'{len(df21_detail)} records'})
    
    # Table 23: Solvency
    print("  - Table 23: Solvency Ratio")
    df23_facts, df23_detail = extract_table_23(part1_path, name_xwalk)
    if len(df23_facts) > 0:
        all_facts.append(df23_facts)
    if len(df23_detail) > 0:
        table_23_records.append(df23_detail)
        qa_logs.append({'Check': 'Table 23 Extraction', 'Status': 'PASS', 'Details': f'{len(df23_detail)} records'})
    
    # Table 28: Persistency
    print("  - Table 28: Persistency")
    df28 = extract_table_28(part1_path, name_xwalk)
    if len(df28) > 0:
        all_facts.append(df28)
        qa_logs.append({'Check': 'Table 28 Extraction', 'Status': 'PASS', 'Details': f'{len(df28)} records'})
    
    # Table 29: Number of Offices (State-wise)
    print("  - Table 29: Number of Offices")
    df29 = extract_table_29(part1_path, name_xwalk)
    if len(df29) > 0:
        state_breakdown_records.append(df29)
        qa_logs.append({'Check': 'Table 29 Extraction', 'Status': 'PASS', 'Details': f'{len(df29)} state-level records'})
    
    # Part V extractions
    print("\nExtracting Part V tables...")
    
    # Table 100: Individual by Channel
    print("  - Table 100: Individual by Channel")
    df100 = extract_table_100(part5_path, name_xwalk)
    if len(df100) > 0:
        all_facts.append(df100)
        qa_logs.append({'Check': 'Table 100 Extraction', 'Status': 'PASS', 'Details': f'{len(df100)} records'})
    
    # Table 102: Group by Channel
    print("  - Table 102: Group by Channel")
    df102 = extract_table_102(part5_path, name_xwalk)
    if len(df102) > 0:
        all_facts.append(df102)
        qa_logs.append({'Check': 'Table 102 Extraction', 'Status': 'PASS', 'Details': f'{len(df102)} records'})
    
    # Combine all facts
    print("\nCombining and validating data...")
    facts_df = pd.concat(all_facts, ignore_index=True) if all_facts else pd.DataFrame()
    
    # Deduplicate
    if len(facts_df) > 0:
        original_count = len(facts_df)
        facts_df = facts_df.drop_duplicates()
        dedup_count = original_count - len(facts_df)
        if dedup_count > 0:
            qa_logs.append({'Check': 'Deduplication', 'Status': 'INFO', 'Details': f'Removed {dedup_count} duplicates'})
    
    # Validate
    validate_facts_table(facts_df, qa_logs)
    
    # Combine state breakdown
    state_df = pd.concat(state_breakdown_records, ignore_index=True) if state_breakdown_records else pd.DataFrame()
    
    # Combine table 21/23
    table_21_df = pd.concat(table_21_records, ignore_index=True) if table_21_records else pd.DataFrame()
    table_23_df = pd.concat(table_23_records, ignore_index=True) if table_23_records else pd.DataFrame()
    
    # Create name crosswalk dataframe
    xwalk_df = pd.DataFrame([
        {'Original_Name': k, 'Standardized_Name': v}
        for k, v in name_xwalk.items()
    ])
    
    # Create QA logs dataframe
    qa_df = pd.DataFrame(qa_logs)
    
    # Create data dictionary
    data_dict = pd.DataFrame([
        {'Column': 'Insurer', 'Description': 'Standardized insurer name', 'Type': 'String', 'Notes': 'See name_xwalk for original to standardized mapping'},
        {'Column': 'Year', 'Description': 'Fiscal year ending (e.g., 2024 = FY 2023-24)', 'Type': 'Integer', 'Notes': 'Extracted from "YYYY-YY" or "as on 31 March YYYY" format'},
        {'Column': 'L1', 'Description': 'Product category Level 1', 'Type': 'String', 'Notes': 'Values: Linked, Non-Linked, or blank'},
        {'Column': 'L2', 'Description': 'Product category Level 2', 'Type': 'String', 'Notes': 'Values: Participating, Non-Participating, or blank'},
        {'Column': 'L3', 'Description': 'Product category Level 3', 'Type': 'String', 'Notes': 'Values: Life, Annuity, Pension, Health, or blank'},
        {'Column': 'Individual_Group', 'Description': 'Business segment', 'Type': 'String', 'Notes': 'Values: Individual, Group, Not Applicable'},
        {'Column': 'Distribution_Channel', 'Description': 'Sales channel', 'Type': 'String', 'Notes': 'Values: Individual Agents, Corporate Agents - Banks, Corporate Agents - Others, Brokers, Direct Selling, MI Agents, CSCs, Web Aggregators, IMF, Online, POS, Others, Not Applicable'},
        {'Column': 'KPI', 'Description': 'Key Performance Indicator', 'Type': 'String', 'Notes': 'Total Premium, New Business Premium, New Business Policy, Total Policy (Year-End), Sum Assured (Year-End), Assets Under Management, Solvency Ratio, Persistency (13M/25M/37M/49M/61M, Policy/Premium), Number of Offices'},
        {'Column': 'Value', 'Description': 'Metric value', 'Type': 'Float', 'Notes': 'Units: Premium/Sum Assured/AUM in â‚¹ absolute (converted from Crore Ã— 10,000,000); Policies/Offices as integers; Persistency 0-100; Solvency as-is'},
        {'Column': 'Source', 'Description': 'Source table reference', 'Type': 'String', 'Notes': 'Table number from IRDAI handbook'},
    ])
    
    print(f"\nETL Complete!")
    print(f"  Facts table: {len(facts_df)} records")
    print(f"  State breakdown: {len(state_df)} records")
    print(f"  Table 21/23: {len(table_21_df) + len(table_23_df)} records")
    print(f"  Name crosswalk: {len(xwalk_df)} mappings")
    
    return {
        'facts': facts_df,
        'state_breakdown': state_df,
        'table_21': table_21_df,
        'table_23': table_23_df,
        'name_xwalk': xwalk_df,
        'qa_logs': qa_df,
        'data_dictionary': data_dict
    }

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == '__main__':
    import os
    
    # Paths
    PART1_PATH = './input/Part I.xlsx'
    PART5_PATH = './input/Part V.xlsx'
    OUTPUT_DIR = './output'
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(f'{OUTPUT_DIR}/checks', exist_ok=True)
    
    # Run ETL
    results = run_etl(PART1_PATH, PART5_PATH, OUTPUT_DIR)
    
    # Post-processing: Replace NaN with empty strings in string columns
    string_cols = ['L1', 'L2', 'L3', 'Distribution_Channel']
    for col in string_cols:
        if col in results['facts'].columns:
            results['facts'][col] = results['facts'][col].fillna('')
        if col in results['state_breakdown'].columns:
            results['state_breakdown'][col] = results['state_breakdown'][col].fillna('')
    
    # Save outputs
    print("\nSaving outputs...")
    
    # Main facts table
    results['facts'].to_excel(f'{OUTPUT_DIR}/facts_table.xlsx', index=False)
    
    # State breakdown
    results['state_breakdown'].to_excel(f'{OUTPUT_DIR}/state_breakdown.xlsx', index=False)
    
    # Name crosswalk
    results['name_xwalk'].to_excel(f'{OUTPUT_DIR}/checks/name_xwalk.xlsx', index=False)
    
    # QA logs
    results['qa_logs'].to_excel(f'{OUTPUT_DIR}/checks/qa_logs.xlsx', index=False)
    
    # Data dictionary
    results['data_dictionary'].to_excel(f'{OUTPUT_DIR}/checks/data_dictionary.xlsx', index=False)
    
    print(f"\nAll outputs saved to {OUTPUT_DIR}")
