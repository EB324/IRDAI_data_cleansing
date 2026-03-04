# IRDAI Data Cleansing

ETL pipeline for extracting, transforming, and standardizing data from IRDAI (Insurance Regulatory and Development Authority of India) Insurance Handbook workbooks into analysis-ready outputs.

## Overview

This project processes insurance industry data from IRDAI handbooks (Part I and Part V) and transforms them into standardized, structured datasets suitable for analysis and reporting. The pipeline handles:

- Premium data (total and new business)
- Policy counts and sum assured
- State-wise distribution
- Distribution channel breakdown
- Assets under management
- Solvency ratios
- Persistency metrics
- Office locations

The pipeline is backward-compatible and has been tested against both the FY 2023-24 and FY 2024-25 handbook editions.

## Project Structure

```
IRDAI_data_cleansing/
├── data_treatment.py         # Main ETL pipeline script
├── requirements.txt           # Python dependencies
├── .gitignore                # Git ignore rules (excludes .xlsx files)
├── input/                    # Input data folder
│   ├── Part I.xlsx          # IRDAI Handbook Part I (current year)
│   ├── Part V.xlsx          # IRDAI Handbook Part V (current year)
│   └── 2023-24 archive/    # Previous year's data for reference
│       ├── Part I.xlsx
│       └── Part V.xlsx
└── output/                   # Generated outputs
    ├── facts_table.xlsx     # Main facts table
    ├── state_breakdown.xlsx  # State-level data
    └── checks/              # Quality assurance outputs
        ├── name_xwalk.xlsx  # Insurer name standardization mapping
        ├── qa_logs.xlsx     # Data quality validation logs
        └── data_dictionary.xlsx  # Schema documentation
```

## Installation

### Prerequisites

- Python 3.7 or higher
- pip package manager

### Setup

1. Clone or download this repository

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

### Input Requirements

Place the following files in the `input/` directory:
- `Part I.xlsx` - IRDAI Insurance Handbook Part I
- `Part V.xlsx` - IRDAI Insurance Handbook Part V

### Running the Pipeline

```bash
python data_treatment.py
```

The script will:
1. Extract data from multiple tables in both input files
2. Standardize insurer names and categories
3. Convert units (Crores to absolute Rupees, thousands to absolute numbers)
4. Validate data quality
5. Generate output files in the `output/` directory

## Output Files

### Main Outputs

**`facts_table.xlsx`**
- Unified fact table with all KPIs
- Columns: Insurer, Year, L1, L2, L3, Individual_Group, Distribution_Channel, KPI, Value, Source
- All financial values in absolute Rupees (₹)

**`state_breakdown.xlsx`**
- State-wise breakdown of business metrics
- Includes data from Tables 6 (Individual), 8 (Group), and 29 (Offices)

### Quality Control Outputs (in `checks/` subfolder)

**`name_xwalk.xlsx`**
- Mapping of original insurer names to standardized names
- Useful for data lineage and verification

**`qa_logs.xlsx`**
- Data quality validation results
- Includes checks for column completeness, value ranges, null counts, etc.

**`data_dictionary.xlsx`**
- Complete schema documentation
- Column definitions, data types, and notes

## Data Sources

The pipeline extracts data from the following tables:

### Part I Tables
- **Table 2**: Total Premium by Insurer
- **Table 3**: New Business Premium by Insurer
- **Table 6**: State-wise Individual New Business
- **Table 8**: State-wise Group Business
- **Table 10**: Individual Business in Force (Policies)
- **Table 11**: Sum Assured of Policies in Force
- **Table 12**: Linked and Non-Linked Premium Breakdown
- **Table 21**: Assets Under Management
- **Table 23**: Solvency Ratio
- **Table 28**: Persistency Ratios
- **Table 29**: State-wise Distribution of Offices

### Part V Tables
- **Table 100**: Individual New Business by Distribution Channel (single-year; year auto-detected from title)
- **Table 102**: Group New Business by Distribution Channel (single-year; year auto-detected from title)

## Key Features

### Data Standardization
- **Insurer Names**: Fuzzy matching algorithm standardizes insurer names across sources
- **State Names**: Direct mapping standardizes state name variations, spelling errors, and union territory mergers
- **Product Categories**: Three-level categorization (L1: Linked/Non-Linked, L2: Participating/Non-Participating, L3: Life/Annuity/Pension/Health)
- **Distribution Channels**: Normalized channel names (Individual Agents, Banks, Brokers, etc.)

### Unit Conversions
- Premium, Sum Assured, AUM: Crores → Absolute Rupees (× 10,000,000)
- Policy counts: Thousands → Absolute numbers (× 1,000)
- Persistency: Normalized to 0-100 scale

### Data Quality
- Automated validation checks
- Duplicate detection and removal
- Value range verification (e.g., Persistency 0-100)
- Null value tracking

## KPIs Captured

- Total Premium
- New Business Premium
- Renewal Premium
- New Business Policy (count)
- Total Policy (Year-End)
- Sum Assured (Year-End)
- Assets Under Management
- Solvency Ratio
- Persistency (13M, 25M, 37M, 49M, 61M - Policy based)
- Number of Offices

## KPI × Granularity × Source Matrix

The following matrix shows which KPIs are available at different levels of granularity and their source tables. Year ranges shown are for the FY 2024-25 handbook; the FY 2023-24 handbook covers one year earlier (e.g., 2016-2025 becomes 2015-2024):

| KPI | Year Range | Individual/Group | Product Category (L1/L2/L3) | Distribution Channel | State Breakdown | Source Table(s) |
|-----|------------|------------------|------------------------------|----------------------|-----------------|------------------|
| **Total Premium** | 2016-2025 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 2 |
| **Total Premium** | 2016-2025 | Not Applicable | ✅ L1 only (Linked/Non-Linked) | ❌ | ❌ | Part I - Table 12 |
| **New Business Premium** | 2016-2025 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 3 |
| **New Business Premium** | 2016-2025 | Not Applicable | ✅ L1 only (Linked/Non-Linked) | ❌ | ❌ | Part I - Table 12 |
| **New Business Premium** | 2024-2025 | Individual | ❌ | ❌ | ✅ State | Part I - Table 6 |
| **New Business Premium** | 2024-2025 | Group | ❌ | ❌ | ✅ State | Part I - Table 8 |
| **New Business Premium** | 2025 only | Individual | ❌ | ✅ All Channels | ❌ | Part V - Table 100 |
| **New Business Premium** | 2025 only | Group | ❌ | ✅ All Channels | ❌ | Part V - Table 102 |
| **Renewal Premium** | 2016-2025 | Not Applicable | ✅ L1 only (Linked/Non-Linked) | ❌ | ❌ | Part I - Table 12 |
| **New Business Policy** | 2024-2025 | Individual | ❌ | ❌ | ✅ State | Part I - Table 6 |
| **New Business Policy** | 2025 only | Individual | ❌ | ✅ All Channels | ❌ | Part V - Table 100 |
| **Total Policy (Year-End)** | 2017-2025 | Individual | ✅ L1/L2/L3 | ❌ | ❌ | Part I - Table 10 |
| **Sum Assured (Year-End)** | 2017-2025 | Individual | ✅ L1/L2/L3 | ❌ | ❌ | Part I - Table 11 |
| **Assets Under Management** | 2022-2025 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 21 |
| **Solvency Ratio** | 2016-2025 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 23 |
| **Persistency (13M/25M/37M/49M/61M)** | 2016-2025 | Individual | ❌ | ❌ | ❌ | Part I - Table 28 |
| **Number of Offices** | 2016-2025 | Not Applicable | ❌ | ❌ | ✅ State | Part I - Table 29 |

### Granularity Definitions

**Individual/Group**: Business segment categorization
- `Individual`: Retail/individual policies
- `Group`: Corporate/group schemes
- `Not Applicable`: KPI applies to both or neither

**Product Category (L1/L2/L3)**: Three-level product classification
- **L1**: `Linked` or `Non-Linked`
- **L2**: `Participating` or `Non-Participating` (rarely populated)
- **L3**: `Life`, `Annuity`, `Pension`, or `Health`

**Distribution Channel**: Sales channel categorization (12 channels)
- Individual Agents
- Corporate Agents - Banks
- Corporate Agents - Others
- Brokers
- Direct Selling
- MI Agents
- CSCs (Common Service Centres)
- Web Aggregators
- IMF (Insurance Marketing Firms)
- Online
- POS (Point of Sales)
- Others

**State Breakdown**: Geographic breakdown by Indian state/union territory (available in separate `state_breakdown.xlsx` file)

### Notes on Granularity
- Each handbook edition provides a rolling 10-year window for most tables. The FY 2024-25 edition covers FY 2015-16 through FY 2024-25; the FY 2023-24 edition covered FY 2014-15 through FY 2023-24.
- Distribution channel breakdowns (Tables 100/102) are single-year snapshots; the year is auto-detected from the table title.
- Product category breakdowns (L1/L2/L3) are available for in-force business metrics (Tables 10 & 11)
- State-level breakdowns are maintained in a separate output file: `state_breakdown.xlsx`

## Data Mappings

### Insurer Name Mapping

The ETL pipeline standardizes various insurer name formats into canonical names. Multiple source name variations are mapped to a single display name. The dictionary handles rebrandings, IRDAI typos, and format inconsistencies across tables and handbook editions:

| Display Name | Source Name(s) |
|-------------|----------------|
| LIC | life insurance corporation of india, lic of india, lic |
| ABSLI | aditya birla sunlife insurance company ltd, aditya birla sun life insurance company ltd, aditya birla sun life insurance company limited, aditya birla sunlife, aditya birla sun life, aditya birla sunlife insurance co ltd |
| ICICI Pru Life | icici prudential life insurance company ltd, icici prudential life insurance, icici prudential life, icici prudential, icici pru life |
| SBI Life | sbi life insurance company ltd, sbi life insurance company limited, sbi life insurance, sbi life |
| MaxLife | max life insurance company ltd, max life insurance, maxlife insurance company ltd, maxlife, axis maxlife  insurance company ltd, axis maxlife insurance company ltd, axis max life insurance company ltd, axis max life insurance limited, axis max life insurance, axis max life, axis maxlife |
| Tata AIA | tata aia life insurance company ltd, tata aia life insurance co ltd, tata aia life insurance co. ltd, tata aia life insurance, tata aia life, tata aia |
| PNB Metlife | pnb metlife india insurance company ltd, pnb metlife india insurance co ltd, pnb metlife insurance co ltd, pnb metlife india insurance, pnb metlife |
| Canara HSBC | canara hsbc obc life insurance company ltd, canara hsbc life insurance company ltd, canara hsbc obc life insurance, canara hsbc life insurance, canara hsbc obc life, canara hsbc life, canara hsbc obc, canara hsbc |
| HDFC Life | hdfc life insurance company ltd, hdfc life insurance, hdfc life, hdfc |
| Kotak Life | kotak mahindra life insurance ltd, kotak mahindra life insurance, kotak mahindra life, kotak mahindra, kotak mahindra om life insurance co ltd, kotak mahindra om life insurance, kotak mahindra om life, kotak life, kotak |
| Bajaj Allianz Life | bajaj allianz life insurance company ltd, bajaj allianz life insurance co ltd, bajaj allianz life insurance, bajaj allianz life, bajaj allianz |
| Bharti AXA Life | bharti axa life insurance company ltd, bharti axa life insurance co ltd, bharti axa life insurance, bharti axa life, bharti-axa life insurance co ltd, bharti-axa life, bharti-axa, bharti axa |
| Exide Life | exide life insurance company ltd, exide life insurance co ltd, exide life insurance, exide life, exide |
| Aviva Life | aviva life insurance company india ltd, aviva life insurance company india limited, aviva life insurance, aviva life, aviva |
| Ageas Federal Life | ageas federal life insurance company ltd, ageas federal life insurance co ltd, aegas federal life insurance co ltd, aegas federal life insurance company limited, ageas federal life insurance company limited, ageas federal life insurance, ageas federal life, aegas federal life, ageas federal, aegas federal, ageas |
| Future Generali Life | future generali india life insurance company ltd, future generali india life insurance company limited, future generali india life insurance, future generali life, future generali |
| Edelweiss Tokio Life | edelweiss tokio life insurance company ltd, edelweiss tokio life insurance, edelweiss tokio life, edelweiss tokio, edelweiss life insurance company ltd, edelweiss life insurance company limited, edelweiss life insurance co ltd, edelweiss life insurance, edelweiss life |
| IndiaFirst Life | indiafirst life insurance company ltd, indiafirst life insurance company limited, indiafirst life insurance limited, indiafirst life insurance, indiafirst life, indiafirst |
| Bandhan Life | bandhan life insurance company ltd, bandhan life insurance ltd, bandhan life insurance company limited, bandhan life insurance limited, bandhan life insurance, bandhan life, bandhan |
| Acko Life | acko life insurance ltd, acko life insurance, acko life insurance limited, acko life |
| Credit Access Life | credit access life insurance ltd, creditaccess life insurance ltd, credit access life insurance company limited, credit access life insurance, credit access life, creditaccess life, cal |
| Go Digit Life | go digit life, go digit life insurance, go digit life insurance limited, go digit life insurance company limited, go digit life insurance ltd, godigit, godigit life |
| Pramerica Life | pramerica life insurance company ltd, pramerica life insurance ltd, pramerica life insurance company limited, pramerica life insurance limited, pramerica life insurance, pramerica life, pramerica |
| Reliance Nippon Life | reliance nippon life insurance company ltd, reliance nippon life insurance company limited, reliance nippon life insurance, reliance nippon life, reliance nippon |
| Sahara India Life | sahara india life insurance company ltd, sahara india life insurance, sahara india life, sahara india, sahara life, sahara |
| Shriram Life | shriram life insurance company ltd, shriram life insurance co ltd, shriram life insurance company limited, shriram life insurance, shriram life, shriram |
| Star Union Dai-ichi Life | star union dai-ichi life insurance company ltd, star union dai-ichi life insurance company, star union dai-ichi life insurance, star union dai-ichi life, star union dai-ichi, sud life |
| Aegon Life | aegon life insurance company ltd, aegon life insurance, aegon life |

*Note: Several insurers were rebranded between the FY 2023-24 and FY 2024-25 handbooks. MaxLife → Axis MaxLife (effective 12 Dec 2024), Canara HSBC OBC → Canara HSBC (dropped "OBC"), Edelweiss Tokio → Edelweiss (dropped "Tokio", effective 3 Jun 2024), Kotak Mahindra → Kotak Mahindra OM (effective 2024-25), Aegon → Bandhan (effective 5 Apr 2024). All historical and current name variants are handled by the dictionary. The display names are kept stable to avoid breaking downstream dependencies.*

*Note: IRDAI source files contain inconsistent name formats across tables. For example, the FY 2024-25 Table 23 uses "Aegas Federal" (typo for Ageas), "Bajaj Allianz Life Insurance Co Ltd" (shortened), and "Kotak Mahindra OM Life Insurance Co. Ltd." (new brand). The name dictionary covers all known variants.*

### State Name Mapping

State names are standardized using the mapping below to handle variations in spelling, formatting, and union territory mergers:

| Display Name | Source Name(s) |
|-------------|----------------|
| Andaman & Nicobar | Andaman and Nicobar, Andaman & Nicobar Islands, Andaman & Nicobar |
| Andhra Pradesh | Andhra Pradesh |
| Arunachal Pradesh | Arunachal Pradesh |
| Assam | Assam |
| Bihar | Bihar |
| Chandigarh | Chandigarh |
| Chhattisgarh | Chattisgarh, Chhattisgarh |
| Dadra and Nagar Haveli and Daman and Diu | Dadra and Nagar Haveli, Dadra and Nagar Haveli and Daman and Diu, Daman and Diu, Daman, Diu, Dadra & Nagar Haveli, Dadra & Nagra Haveli and Daman & Diu |
| NCT of Delhi | Delhi, Delhi (NCT), NCT of Delhi |
| Goa | Goa |
| Gujarat | Gujarat |
| Haryana | Haryana |
| Himachal Pradesh | Himachal Pradesh |
| Jammu & Kashmir | Jammu and Kashmir, Jammu & Kashmir |
| Jharkhand | Jharkand, Jharkhand |
| Karnataka | Karnataka |
| Kerala | Kerala |
| Ladakh | Ladakh |
| Lakshadweep | Lakshadweep |
| Madhya Pradesh | Madhya Pradesh |
| Maharashtra | Maharashtra |
| Manipur | Manipur |
| Meghalaya | Meghalaya |
| Mizoram | Mizoram |
| Nagaland | Nagaland |
| Odisha | Odisha |
| Puducherry | Puducherry |
| Punjab | Punjab |
| Rajasthan | Rajashtan, Rajasthan |
| Sikkim | Sikkim |
| Tamil Nadu | TamilNadu, Tamilnadu, Tamil Nadu |
| Telangana | Telangana |
| Tripura | Tripura |
| Uttar Pradesh | Uttar Pradesh |
| Uttarakhand | Uttarakhand, Uttrakhand |
| West Bengal | West Bengal |

*Note: State standardization is applied to all state-wise data extractions (Tables 6, 8, and 29). The mapping handles common spelling variations (e.g., "Jharkand" → "Jharkhand"), union territory mergers (e.g., Dadra and Nagar Haveli + Daman and Diu), and formatting differences (e.g., "TamilNadu" → "Tamil Nadu").*

## Configuration

Key configuration parameters in `data_treatment.py`:

```python
CRORE_TO_RUPEES = 10_000_000  # Conversion factor
INSURER_CANONICAL_NAMES = {...}  # Name standardization dictionary
STATE_MAPPING = {...}  # State name standardization dictionary
CHANNEL_MAPPING = {...}  # Distribution channel normalization
```

## ⚠️ Hard-Coded Elements to Be Aware Of

The ETL script contains several hard-coded elements that may need adjustment if the source file structure changes. Items marked ✅ have been made backward-compatible as of the FY 2024-25 update.

### 1. ✅ Sheet Names (Backward-Compatible)

Table 11 had a trailing space in its sheet name in the FY 2023-24 source file, which was fixed in FY 2024-25. The script now handles both:
```python
# Tries '11' first (FY 2024-25), falls back to '11 ' (FY 2023-24)
try:
    df = pd.read_excel(xlsx_path, sheet_name='11', header=None)
except ValueError:
    df = pd.read_excel(xlsx_path, sheet_name='11 ', header=None)
```
**Action if changed in future editions**: If the sheet name changes to something else entirely, update the `extract_table_11()` function.

### 2. Hard-Coded Column Mappings

#### Table 100 (Individual by Channel)
```python
CHANNEL_MAP = {
    2: ('Individual Agents', 'Policies'),
    3: ('Individual Agents', 'Premium'),
    4: ('Corporate Agents - Banks', 'Policies'),
    # ... columns 2-25 mapped to specific channels
}
```
**Status**: Column positions have been stable across FY 2023-24 and FY 2024-25. No change needed.

**Action if changed**: If column positions shift in the source file, update the column numbers in the `CHANNEL_MAP` dictionary in `extract_table_100()`

#### Table 102 (Group by Channel)
```python
CHANNEL_MAP = {
    3: ('Individual Agents', 'Premium'),
    6: ('Corporate Agents - Banks', 'Premium'),
    # ... every 3rd column mapped (Schemes, Premium, Lives pattern)
}
```
**Status**: Column positions have been stable across FY 2023-24 and FY 2024-25. No change needed.

**Action if changed**: Update column numbers in `extract_table_102()`. Note that each channel has 3 columns, with Premium in the middle position.

### 3. Header Row Positions

Each extraction function has hard-coded row numbers:
```python
insurer_row = 2    # Row where insurer names appear
year_row = 3       # Row where years appear
metric_row = 4     # Row where metric types appear
data_start = 5     # First row of actual data
```
**Status**: Header row positions have been stable across FY 2023-24 and FY 2024-25 for all extracted tables.

**Action if changed**: If source file adds/removes header rows, update these constants in each `extract_table_X()` function

### 4. ✅ Year Detection for Tables 100/102 (Backward-Compatible)

The year for Tables 100 and 102 is now automatically detected from the table title row:
```python
# Parses year from title, e.g., "... (2024-25)" → 2025
title_val = str(df.iloc[0, 0])
year_match = re.search(r'\((\d{4})-(\d{2})\)', title_val)
if year_match:
    table_year = int(year_match.group(1)) + 1
else:
    table_year = 2024  # fallback
```
**Action if changed**: Only needs updating if IRDAI changes the title format (currently `"TABLE 100: ... (YYYY-YY)"`).

### 5. Category Mapping Strings

Product category detection uses specific string patterns:
```python
category_map = {
    'non linked life business': ('Non-Linked', '', 'Life'),
    'non linked -general annuity business': ('Non-Linked', '', 'Annuity'),  # Note the dash typo
    # ...
}
```
**Status**: All category labels (including the dash typo in "non linked -general") are identical between FY 2023-24 and FY 2024-25. No change needed.

**Action if changed**: If IRDAI changes category labels or fixes typos (like the dash in "non linked -general"), update the `category_map` dictionary in `extract_table_10()` and `extract_table_11()`

### 6. Exclusion Lists

Aggregate row detection uses exclusion keywords:
```python
excluded_insurers = ['grand total', 'private total', 'private sector total',
                     'public sector total', 'total', 'industry total', 'growth rate']
```
**Note**: `'growth rate'` was added for FY 2024-25, which introduced a "GROWTH RATE" column section in Tables 6 and 8 that was not present in FY 2023-24. This exclusion is safe for both editions.

**Action if changed**: If new aggregate row labels are added, update the `excluded_insurers` list in relevant extraction functions

### 7. Column Ranges for Table 12

Linked and Non-Linked premium columns are hard-coded:
```python
column_groups = [
    ('Linked', range(42, 52)),      # Columns 42-51 for Linked e.Total
    ('Non-Linked', range(92, 102)), # Columns 92-101 for Non-Linked e.Total
]
```
**Status**: The "e.Total (c+d)" labels appear at the same 0-indexed column positions (42 and 92) in both FY 2023-24 and FY 2024-25. The year values within those columns shift forward by one year per edition, but the column positions remain stable. No change needed.

**Action if changed**: If column positions shift, update the range values in `extract_table_12()`

### Validation Checklist

When working with a new handbook version:
- [ ] Check if sheet names match (the Table 11 fallback handles `'11'` and `'11 '` automatically)
- [ ] Verify header row positions haven't changed
- [ ] Confirm column positions for Tables 100 and 102
- [ ] Verify that the Table 100/102 title format still allows automatic year detection
- [ ] Review category label strings for typos or changes
- [ ] Check if new aggregate row labels or column sections were introduced (like "GROWTH RATE")
- [ ] Check for insurer rebrandings or name changes; add new variants to `INSURER_CANONICAL_NAMES`
- [ ] Verify the script runs without errors and review QA logs
- [ ] Check `name_xwalk.xlsx` output for any unmatched insurers (names mapped to title-case originals rather than canonical names)

## Backward Compatibility

The script has been designed to work with both the FY 2023-24 and FY 2024-25 IRDAI Handbook editions. Key differences handled:

| Aspect | FY 2023-24 | FY 2024-25 | How Handled |
|--------|-----------|-----------|-------------|
| Table 11 sheet name | `'11 '` (trailing space) | `'11'` (no space) | Try/except fallback |
| Tables 100/102 year | 2024 | 2025 | Auto-detected from title |
| Year ranges | 2014-15 to 2023-24 | 2015-16 to 2024-25 | Dynamic year parsing |
| MaxLife name | `MaxLife Insurance Company Ltd.` | `Axis MaxLife Insurance Company Ltd.` | Both in name dictionary → `MaxLife` |
| Canara HSBC name | `Canara HSBC OBC Life...` | `Canara HSBC Life...` | Both in name dictionary → `Canara HSBC` |
| Edelweiss name | `Edelweiss Tokio Life...` | `Edelweiss Life...` (in some tables) | Both in name dictionary → `Edelweiss Tokio Life` |
| Kotak name | `Kotak Mahindra Life...` | `Kotak Mahindra OM Life...` | Both in name dictionary → `Kotak Life` |
| Ageas typo | `Ageas Federal Life...` | `Aegas Federal Life...` (in T23/T28) | Both in name dictionary → `Ageas Federal Life` |
| Table 6/8 "GROWTH RATE" section | Not present | Present as column group | Added to exclusion list |
| Table 36 (Rural/Social Sector) | Present | Removed | Not extracted by pipeline |
| Insurer ordering in Table 2 | Non-alphabetical | Alphabetical | Parsed by name, not position |
| Table 6/8 new-entrant columns | 2 cols (1 year) for Acko/Credit Access/Go Digit | 4 cols (2 years) for all | Dynamic column parsing |

## Notes

- Fiscal years are represented by their ending year (e.g., 2024 = FY 2023-24, 2025 = FY 2024-25)
- Negative values are included in the output (e.g., for adjustments or reversals)
- The pipeline uses fuzzy matching (85% threshold) for insurer name standardization
- Excel files (.xlsx) are excluded from version control via .gitignore

## Troubleshooting

**Missing input files**: Ensure `Part I.xlsx` and `Part V.xlsx` are in the `input/` directory

**Import errors**: Run `pip install -r requirements.txt` to install all dependencies

**Empty output**: Check the QA logs in `output/checks/qa_logs.xlsx` for extraction issues

**Unrecognized insurer names**: Check `output/checks/name_xwalk.xlsx` for any names that were not matched to canonical names (they will appear as title-cased originals). Add new variants to `INSURER_CANONICAL_NAMES` as needed.

**Sheet not found error**: If a Table 11 extraction fails, check whether the sheet is named `'11'` or `'11 '` (with trailing space). The script handles both, but other sheets could have similar issues in future editions.

## License

Internal use only - Ageas Asia Services Limited

## Contact

For questions or issues, please contact the Data & AI team.
