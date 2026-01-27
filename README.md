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

## Configuration

Key configuration parameters in `data_treatment.py`:

```python
CRORE_TO_RUPEES = 10_000_000  # Conversion factor
INSURER_CANONICAL_NAMES = {...}  # Name standardization dictionary
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
