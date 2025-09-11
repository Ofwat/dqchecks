# dqchecks

**Data quality validation service** for checking submitted Excel files against defined expectations.

This library is used to validate Excel spreadsheets submitted to Ofwat, ensuring they conform to an expected structure and content, using a provided template.

---

## Installation

```bash
pip install ofwat-dqchecks
```

## Overview

There are two types of checks:

1. Template-based validation – compares a submitted Excel file against a reference template.
2. Standalone validation – runs checks on a single Excel file without comparing it to a template.

Due to how openpyxl handles formula and value parsing, both the template and the submitted file must be opened twice:

- Once with `data_only=False` to access formulas
- Once with `data_only=True` to access evaluated values

All outputs are returned as a standardised pandas `DataFrame`.

## Example Usage
### 1. Load Workbooks
```python
import openpyxl
import dqchecks

file_path = "path/to/company_file.xlsx"
template_path = "path/to/template_file.xlsx"

# Load with and without formulas
wb_template = openpyxl.open(template_path, data_only=False)
wb_template_dataonly = openpyxl.open(template_path, data_only=True)

wb_company = openpyxl.open(file_path, data_only=False)
wb_company_dataonly = openpyxl.open(file_path, data_only=True)
```

### 2. Rule 1: Formula Difference

This check compares formulas cell-by-cell between the company file and the template for all overlapping sheets (i.e. sheets with matching names). It flags differences in formulas between the two workbooks.

> Note: If the Excel sheets have mismatched active regions (used ranges), this check may fail.

```python
dqchecks.panacea.find_formula_differences(wb_template, wb_company)
```

### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category     | Error_Severity | Error_Desc                                                                                                                |
|-----------|----------------|---------------------------|---------|--------------------|----------------|---------------------------------------------------------------------------------------------------------------------------|
| 9a0cdce1  | F_Outputs 9 OK | Rule 1: Formula Difference | A4      | Formula Difference | hard           | Template: F_Outputs 9 OK!A4 (Formula: ='F_Outputs 1 OK'!A4) != Company: F_Outputs 9 OK!A4 (Value: ¬¬'F_Outp_


### 3. Rule 2: Formula Error Check
This check scans the entire workbook and identifies any cells containing Excel formula errors (e.g., #DIV/0!, #VALUE!, #REF!, etc.). Each cell with an error is returned as a separate row in the output dataframe, with a unique Event_Id.

```python
 dqchecks.panacea.find_formula_errors(wb_company_dataonly)
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce2  | F_Outputs 9 OK | Rule 2: Formula Error Check | A4      | Formula Error   | hard           | #DIV/0!    |


### 4. Rule 3: Missing Sheets

This check compares the submitted workbook (`wb_company_dataonly`) against the template, and returns a DataFrame listing any sheets that are present in the template but missing from the submitted file.

> Note: This check only identifies missing sheets. It does **not** detect extra sheets added in the submitted file.

```python
dqchecks.panacea.find_missing_sheets(wb_template_dataonly, wb_company_dataonly)
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce3  | F_Outputs 9 OK | Rule 3: Missing Sheets |       | Missing Sheet   | hard           | Missing Sheet    |


### 4. Rule 4: Structural Discrepancy

This check compares the shape (i.e. number of rows and columns) of each sheet in the submitted workbook (`wb_company_dataonly`) against the corresponding sheet in the template. It helps detect added or removed rows/columns.

> Note: Excel tracks an internal "used range" for each worksheet. This can include cells that appear empty but were previously populated. If data is added and then deleted, the worksheet's shape may still reflect those cells. To address this, we scan each sheet to find the last row and column that contain actual data, and use those dimensions for comparison.

```python
dqchecks.panacea.find_shape_differences(wb_template, wb_company)
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce4  | F_Outputs 9 OK | Rule 4: Structural Discrepancy |       | Structure Discrepancy   | hard           | Template file has 49 rows and 7 columns, Company file has 54 rows and 7 columns.    |


### 5. Rule 5: Boncode Repetition Check

This check scans sheets whose names match a given regex pattern (e.g. `^fOut_`), attempts to read them as flat tables, and validates that a given column (e.g. `Reference`) contains **unique** values — similar to enforcing a primary key constraint in databases.

> Note: This check assumes the sheet follows a flat table structure. You can configure the number of rows above/below the header row to accommodate formatting quirks.

```python
dqchecks.panacea.find_pk_errors(wb_company_dataonly, '^fOut_', 'Reference')
```
#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce5  | F_Outputs 9 OK | Rule 5: Boncode Repetition |       | Duplicate Value   | ?           | Duplicate [Reference] value '123' found in rows [2,3,4].    |



### 6. Rule 6: Missing Boncode Check

This rule uses the same mechanism as Rule 5 but checks for null or missing values in the specified primary key column.

```python
dqchecks.panacea.find_pk_errors(wb_company_dataonly, '^fOut_', 'Reference')
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce6  | F_Outputs 9 OK | Rule 6: Missing Boncode Check |       | Missing Values   | ?           | Rows [2,3,5,8] have missing values in [Reference].    |

