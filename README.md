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

## Project Structure

```
IRDAI_data_cleansing/
├── data_treatment.py         # Main ETL pipeline script
├── requirements.txt           # Python dependencies
├── .gitignore                # Git ignore rules (excludes .xlsx files)
├── input/                    # Input data folder
│   ├── Part I.xlsx          # IRDAI Handbook Part I
│   └── Part V.xlsx          # IRDAI Handbook Part V
└── output/                   # Generated outputs
    ├── facts_table.xlsx     # Main facts table
    ├── state_breakdown_t6_t8.xlsx  # State-level data
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

**`state_breakdown_t6_t8.xlsx`**
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
- **Table 100**: Individual New Business by Distribution Channel (2023-24)
- **Table 102**: Group New Business by Distribution Channel (2023-24)

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

The following matrix shows which KPIs are available at different levels of granularity and their source tables:

| KPI | Year Range | Individual/Group | Product Category (L1/L2/L3) | Distribution Channel | State Breakdown | Source Table(s) |
|-----|------------|------------------|------------------------------|----------------------|-----------------|------------------|
| **Total Premium** | 2015-2024 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 2 |
| **Total Premium** | 2015-2024 | Not Applicable | ✅ L1 only (Linked/Non-Linked) | ❌ | ❌ | Part I - Table 12 |
| **New Business Premium** | 2015-2024 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 3 |
| **New Business Premium** | 2015-2024 | Not Applicable | ✅ L1 only (Linked/Non-Linked) | ❌ | ❌ | Part I - Table 12 |
| **New Business Premium** | 2015-2024 | Individual | ❌ | ❌ | ✅ State | Part I - Table 6 |
| **New Business Premium** | 2015-2024 | Group | ❌ | ❌ | ✅ State | Part I - Table 8 |
| **New Business Premium** | 2024 only | Individual | ❌ | ✅ All Channels | ❌ | Part V - Table 100 |
| **New Business Premium** | 2024 only | Group | ❌ | ✅ All Channels | ❌ | Part V - Table 102 |
| **Renewal Premium** | 2015-2024 | Not Applicable | ✅ L1 only (Linked/Non-Linked) | ❌ | ❌ | Part I - Table 12 |
| **New Business Policy** | 2015-2024 | Individual | ❌ | ❌ | ✅ State | Part I - Table 6 |
| **New Business Policy** | 2024 only | Individual | ❌ | ✅ All Channels | ❌ | Part V - Table 100 |
| **Total Policy (Year-End)** | 2015-2024 | Individual | ✅ L1/L2/L3 | ❌ | ❌ | Part I - Table 10 |
| **Sum Assured (Year-End)** | 2015-2024 | Individual | ✅ L1/L2/L3 | ❌ | ❌ | Part I - Table 11 |
| **Assets Under Management** | 2015-2024 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 21 |
| **Solvency Ratio** | 2015-2024 | Not Applicable | ❌ | ❌ | ❌ | Part I - Table 23 |
| **Persistency (13M/25M/37M/49M/61M)** | 2015-2024 | Individual | ❌ | ❌ | ❌ | Part I - Table 28 |
| **Number of Offices** | 2015-2024 | Not Applicable | ❌ | ❌ | ✅ State | Part I - Table 29 |

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
- Most KPIs are available from FY 2014-15 (ending 2015) through FY 2023-24 (ending 2024)
- Distribution channel breakdowns are only available for FY 2023-24 (2024) from Part V
- Product category breakdowns (L1/L2/L3) are available for in-force business metrics (Tables 10 & 11)
- State-level breakdowns are maintained in a separate output file: `state_breakdown.xlsx`

## Data Mappings

### Insurer Name Mapping

The ETL pipeline standardizes various insurer name formats into canonical names. Multiple source name variations are mapped to a single display name:

| Display Name | Source Name(s) |
|-------------|----------------|
| LIC | life insurance corporation of india, lic of india, lic |
| ABSLI | aditya birla sunlife insurance company ltd, aditya birla sun life insurance company ltd, aditya birla sunlife, aditya birla sun life, aditya birla sunlife insurance co ltd |
| ICICI Pru Life | icici prudential life insurance company ltd, icici prudential life insurance, icici prudential life, icici prudential, icici pru life |
| SBI Life | sbi life insurance company ltd, sbi life insurance, sbi life |
| MaxLife | max life insurance company ltd, max life insurance, maxlife insurance company ltd, maxlife |
| Tata AIA | tata aia life insurance company ltd, tata aia life insurance, tata aia life, tata aia |
| PNB Metlife | pnb metlife india insurance company ltd, pnb metlife insurance co ltd, pnb metlife india insurance, pnb metlife |
| Canara HSBC | canara hsbc obc life insurance company ltd, canara hsbc life insurance company ltd, canara hsbc obc life insurance, canara hsbc life insurance, canara hsbc obc life, canara hsbc life, canara hsbc obc, canara hsbc |
| HDFC Life | hdfc life insurance company ltd, hdfc life insurance, hdfc life, hdfc |
| Kotak Life | kotak mahindra life insurance ltd, kotak mahindra life insurance, kotak mahindra life, kotak mahindra, kotak life, kotak |
| Bajaj Allianz Life | bajaj allianz life insurance company ltd, bajaj allianz life insurance, bajaj allianz life, bajaj allianz |
| Bharti AXA Life | bharti axa life insurance company ltd, bharti axa life insurance co ltd, bharti axa life insurance, bharti axa life, bharti-axa life insurance co ltd, bharti-axa life, bharti-axa, bharti axa |
| Exide Life | exide life insurance company ltd, exide life insurance co ltd, exide life insurance, exide life, exide |
| Aviva Life | aviva life insurance company india ltd, aviva life insurance, aviva life, aviva |
| Ageas Federal Life | ageas federal life insurance company ltd, ageas federal life insurance co ltd, aegas federal life insurance co ltd, ageas federal life insurance, ageas federal life, aegas federal life, ageas federal, aegas federal, ageas |
| Future Generali Life | future generali india life insurance company ltd, future generali india life insurance, future generali life, future generali |
| Edelweiss Tokio Life | edelweiss tokio life insurance company ltd, edelweiss tokio life insurance, edelweiss tokio life, edelweiss tokio |
| IndiaFirst Life | indiafirst life insurance company ltd, indiafirst life insurance, indiafirst life, indiafirst |
| Bandhan Life | bandhan life insurance company ltd, bandhan life insurance ltd, bandhan life insurance, bandhan life, bandhan |
| Acko Life | acko life insurance ltd, acko life insurance, acko life |
| Credit Access Life | credit access life insurance ltd, creditaccess life insurance ltd, credit access life, creditaccess life, cal |
| Go Digit Life | go digit life, go digit life insurance, go digit life insurance limited, godigit |
| Pramerica Life | pramerica life insurance company ltd, pramerica life insurance ltd, pramerica life insurance, pramerica life, pramerica |
| Reliance Nippon Life | reliance nippon life insurance company ltd, reliance nippon life insurance, reliance nippon life, reliance nippon |
| Sahara India Life | sahara india life insurance company ltd, sahara india life insurance, sahara india life, sahara india, sahara life, sahara |
| Shriram Life | shriram life insurance company ltd, shriram life insurance, shriram life, shriram |
| Star Union Dai-ichi Life | star union dai-ichi life insurance company ltd, star union dai-ichi life insurance, star union dai-ichi life, star union dai-ichi, sud life |
| Aegon Life | aegon life insurance company ltd, aegon life insurance, aegon life |

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

The ETL script contains several hard-coded elements that may need adjustment if the source file structure changes:

### 1. Sheet Names with Typos
```python
# Table 11 has a trailing space in the sheet name (typo in source file)
df = pd.read_excel(xlsx_path, sheet_name='11 ', header=None)  # Note the space after '11'
```
**Action if changed**: Update the `sheet_name` parameter in `extract_table_11()` function

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
**Action if changed**: If column positions shift in the source file, update the column numbers in the `CHANNEL_MAP` dictionary in `extract_table_100()`

#### Table 102 (Group by Channel)
```python
CHANNEL_MAP = {
    3: ('Individual Agents', 'Premium'),
    6: ('Corporate Agents - Banks', 'Premium'),
    # ... every 3rd column mapped (Schemes, Premium, Lives pattern)
}
```
**Action if changed**: Update column numbers in `extract_table_102()`. Note that each channel has 3 columns, with Premium in the middle position.

### 3. Header Row Positions

Each extraction function has hard-coded row numbers:
```python
insurer_row = 2    # Row where insurer names appear
year_row = 3       # Row where years appear
metric_row = 4     # Row where metric types appear
data_start = 5     # First row of actual data
```
**Action if changed**: If source file adds/removes header rows, update these constants in each `extract_table_X()` function

### 4. Hard-Coded Years

Tables 100 and 102 are hard-coded to year 2024:
```python
records.append({
    'Insurer': insurer,
    'Year': 2024,  # Hard-coded for FY 2023-24
    # ...
})
```
**Action if changed**: For new handbook versions, update the year value in `extract_table_100()` and `extract_table_102()`

### 5. Category Mapping Strings

Product category detection uses specific string patterns:
```python
category_map = {
    'non linked life business': ('Non-Linked', '', 'Life'),
    'non linked -general annuity business': ('Non-Linked', '', 'Annuity'),  # Note the dash typo
    # ...
}
```
**Action if changed**: If IRDAI changes category labels or fixes typos (like the dash in "non linked -general"), update the `category_map` dictionary in `extract_table_10()` and `extract_table_11()`

### 6. Exclusion Lists

Aggregate row detection uses exclusion keywords:
```python
excluded_insurers = ['grand total', 'private total', 'private sector total', 
                     'public sector total', 'total', 'industry total']
```
**Action if changed**: If new aggregate row labels are added, update the `excluded_insurers` list in relevant extraction functions

### 7. Column Ranges for Table 12

Linked and Non-Linked premium columns are hard-coded:
```python
column_groups = [
    ('Linked', range(42, 52)),      # Columns 42-51 for Linked e.Total
    ('Non-Linked', range(92, 102)), # Columns 92-101 for Non-Linked e.Total
]
```
**Action if changed**: If column positions shift, update the range values in `extract_table_12()`

### Validation Checklist

When working with a new handbook version:
- [ ] Check if sheet names match (especially '11 ' with trailing space)
- [ ] Verify header row positions haven't changed
- [ ] Confirm column positions for Tables 100 and 102
- [ ] Update hard-coded year for Tables 100 and 102
- [ ] Review category label strings for typos or changes
- [ ] Check if new aggregate row labels were introduced
- [ ] Verify the script runs without errors and review QA logs

## Notes

- Fiscal years are represented by their ending year (e.g., 2024 = FY 2023-24)
- Negative values are included in the output (e.g., for adjustments or reversals)
- The pipeline uses fuzzy matching (92% threshold) for insurer name standardization
- Excel files (.xlsx) are excluded from version control via .gitignore

## Troubleshooting

**Missing input files**: Ensure `Part I.xlsx` and `Part V.xlsx` are in the `input/` directory

**Import errors**: Run `pip install -r requirements.txt` to install all dependencies

**Empty output**: Check the QA logs in `output/checks/qa_logs.xlsx` for extraction issues

## License

Internal use only - Ageas Asia Services Limited

## Contact

For questions or issues, please contact the Data & AI team.
