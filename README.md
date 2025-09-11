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

| Event_Id | Sheet_Cd       | Rule_Cd                | Cell_Cd   | Error_Category | Error_Severity | Error_Desc                                                                                                                  |
| --------- | --------------- | -------------------------- | --------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 9a0cdce1  | F_Outputs 9 OK | Rule 1: Formula Difference | A4 | Formula Difference     | hard            | Template: F_Outputs 9 OK!A4 (Formula: ='F_Outputs 1 OK'!A4) != Company: F_Outputs 9 OK!A4 (Value: ¬¬'F_Outputs 1 OK'!A4) |

### 3. Rule 2: Formula Error Check
This check scans the entire workbook and identifies any cells containing Excel formula errors (e.g., #DIV/0!, #VALUE!, #REF!, etc.). Each cell with an error is returned as a separate row in the output dataframe, with a unique Event_Id.

```python
 dqchecks.panacea.find_formula_errors(wb_company_dataonly)
```

#### Sample output

| Event_Id | Sheet_Cd       | Rule_Cd              | Cell_Cd      | Error_Category | Error_Severity | Error_Desc                                                                                                                  |
| --------- | --------------- | -------------------------- | --------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 9a0cdce2  | F_Outputs 9 OK | Rule 2: Formula Error Check | A4 | Formula Error      | hard            | #DIV/0! |