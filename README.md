# dqchecks

**Data quality validation service** for checking submitted Excel files against defined expectations.

This library is used to validate Excel spreadsheets submitted to Ofwat, ensuring they conform to an expected structure and content, using a provided template.

---

## Installation

```bash
pip install ofwat-dqchecks
```

## Overview

There are three main types of data quality checks supported by this library:

1. **Template-based validation** – Compares a submitted Excel file against a known template, detecting differences in structure, formulas, or expected values.
2. **Standalone validation** – Runs checks on a single Excel file, without reference to a template. This is useful for general integrity or content checks.
3. **Flat table loading checks** – Performed when reading flat (tabular) data from specific sheets. These include checks for header spacing and primary key-like constraints such as uniqueness and non-nullability.


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


### 5. Rule 4: Structural Discrepancy

This check compares the shape (i.e. number of rows and columns) of each sheet in the submitted workbook (`wb_company_dataonly`) against the corresponding sheet in the template. It helps detect added or removed rows/columns.

> Note: Excel tracks an internal "used range" for each worksheet. This can include cells that appear empty but were previously populated. If data is added and then deleted, the worksheet's shape may still reflect those cells. To address this, we scan each sheet to find the last row and column that contain actual data, and use those dimensions for comparison.

```python
dqchecks.panacea.find_shape_differences(wb_template, wb_company)
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce4  | F_Outputs 9 OK | Rule 4: Structural Discrepancy |       | Structure Discrepancy   | hard           | Template file has 49 rows and 7 columns, Company file has 54 rows and 7 columns.    |


### 6. Rule 5: Boncode Repetition Check

This check scans sheets whose names match a given regex pattern (e.g. `^fOut_`), attempts to read them as flat tables, and validates that a given column (e.g. `Reference`) contains **unique** values — similar to enforcing a primary key constraint in databases.

> Note: This check assumes the sheet follows a flat table structure. You can configure the number of rows above/below the header row to accommodate formatting quirks.

```python
dqchecks.panacea.find_pk_errors(wb_company_dataonly, '^fOut_', 'Reference')
```
#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce5  | F_Outputs 9 OK | Rule 5: Boncode Repetition |       | Duplicate Value   | ?           | Duplicate [Reference] value '123' found in rows [2,3,4].    |



### 7. Rule 6: Missing Boncode Check

This rule uses the same mechanism as Rule 5 but checks for null or missing values in the specified primary key column.

```python
dqchecks.panacea.find_pk_errors(wb_company_dataonly, '^fOut_', 'Reference')
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce6  | F_Outputs 9 OK | Rule 6: Missing Boncode Check |       | Missing Values   | ?           | Rows [2,3,5,8] have missing values in [Reference].    |



### 8. Rule 7: Company Name Selected

This rule checks whether the company name entered in the submitted spreadsheet matches the expected company name (`company_name_full`).

> Note: Because different templates may store the company name in different sheets or cells, this check uses a fallback approach. It looks for the company name in one of several known sheet/cell combinations.

```python
if "SelectCompany" in wb_company_dataonly.sheetnames:
    company_selection_df = dqchecks.panacea.create_dataframe_from_company_selection_check(
        dqchecks.panacea.check_value_in_cell(wb_company_dataonly, "SelectCompany", company_name_full, cell_name="B4")
    )
elif "Quarterly_Data" in wb_company_dataonly.sheetnames:
    company_selection_df = dqchecks.panacea.create_dataframe_from_company_selection_check(
        dqchecks.panacea.check_value_in_cell(wb_company_dataonly, "Quarterly_Data", company_name_full, cell_name="G9")
    )
else:
    company_selection_df = dqchecks.panacea.create_dataframe_from_company_selection_check(
        dqchecks.panacea.check_value_in_cell(wb_company_dataonly, "Validation", company_name_full, cell_name="B5")
    )
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce7  | F_Outputs 9 OK | Rule 7: Company Name Selected |       | Company name mismatch   | ?           | Expected [Cool company] found [Fun company]    |



### 9. Rule 8: Company Acronym Check

This rule is similar to Rule 7 but checks the company acronym (stored in `Organisation_Cd`) instead of the full company name.

It verifies that the value in a specific cell matches the expected organisation code.

```python
dqchecks.panacea.create_dataframe_from_company_acronym_check(
    dqchecks.panacea.check_value_in_cell(wb_company_dataonly, "Quarterly_Data", Organisation_Cd, cell_name="G10")
)
```

#### Sample output

| Event_Id  | Sheet_Cd       | Rule_Cd                  | Cell_Cd | Error_Category | Error_Severity | Error_Desc |
|-----------|----------------|---------------------------|---------|----------------|----------------|------------|
| 9a0cdce7  | F_Outputs 9 OK | Rule 8: Company Acronym Check |   A4  | Company acronym mismatch   | ?           | Expected [ABC] found [EFG]    |


### 10. Preparing to Load Tables into Pandas DataFrames

Before loading your Excel-based tables into Pandas, you need to set up a few configuration elements and context.

```python
import openpyxl
import datetime
import dqchecks
from dqchecks.exceptions import ColumnHeaderValidationError, EmptyRowsPatternCheckError

# Load the Excel workbook
template_path = "<path_to_template>.xlsx"
wb = openpyxl.load_workbook(template_path, data_only=True)

# Initialize the processing context
context = dqchecks.transforms.ProcessingContext(
    org_cd="ABC",
    submission_period_cd="2020-12",
    process_cd="EFG",
    template_version="v1",
    last_modified=datetime.datetime.now()
)

metadata = {
    "Batch_Id":"Batch_Id",
    "Submission_Period_Cd":"Submission_Period_Cd",
    "Process_Cd":"Process_Cd",
    "Template_Version":"Template_Version",
    "Organisation_Cd":"Organisation_Cd",
    "Validation_Processing_Stage":"Validation_Processing_Stage",
}

# Define the process code (should be set earlier in real use)
process_cd = "qd"

# Configure based on process type
if process_cd == "qd":
    config = dqchecks.transforms.FoutProcessConfig(
        observation_patterns=[],
        fout_patterns=[r"Flat File"],
        column_rename_map=dqchecks.transforms.get_qd_column_rename_map(),
        run_validations=False,
        skip_rows=0,
        reshape=False,
    )
elif process_cd == "pcd":
    config = dqchecks.transforms.FoutProcessConfig(
        observation_patterns=[r"\d{4}-\d{2}"],
        fout_patterns=[r"F_Outputs"],
        column_rename_map=dqchecks.transforms.get_default_column_rename_map(),
    )
elif process_cd == "apr":
    config = dqchecks.transforms.FoutProcessConfig(
        observation_patterns=[r'^\s*2[0-9]{3}-[1-9][0-9]\s*$'],
        fout_patterns=["^fOut_", r"^\s*F_Outputs"],
        column_rename_map=dqchecks.transforms.get_default_column_rename_map(),
        run_validations=True,
    )
```


### 11. Loading then Validating Column Headers and Empty Row Patterns

This step uses `process_fout_sheets` to load the Excel data and, as part of that process, validates the structure of the sheets.  
It raises four specific exceptions if expectations are not met:

- `ColumnHeaderValidationError`: when required column headers are missing or incorrect
- `EmptyRowsPatternCheckError`: when the row structure is unexpectedly empty or malformed
- `ColumnHeaderValidationError`: when headers do not contain the following columns in this order "Acronym, Reference, Item Description, Unit, Model"
- `EmptyRowsPatternCheckError`: when we expected an empty row around header, but found data


Any validation failures are captured into an `error_df` for downstream processing.

```python
import uuid

pivoted_df = None
error_df = None

try:
    pivoted_df = dqchecks.transforms.process_fout_sheets(wb, context, config)

except ColumnHeaderValidationError as e:
    error_df = dqchecks.utils.create_validation_event_row_dataframe(
        Event_Id = uuid.uuid4().hex,
        Batch_Id = "Batch_Id",
        Validation_Processing_Stage = "Validation_Processing_Stage",
        Rule_Cd = 'Column header check',
        Error_Desc = ", ".join(e.args),
        Submission_Period_Cd = "Submission_Period_Cd",
        Process_Cd = "Process_Cd",
        Template_Version = "Template_Version",
        Organisation_Cd = "Organisation_Cd",
    )

except EmptyRowsPatternCheckError as e:
    error_df = dqchecks.utils.create_validation_event_row_dataframe(
        Event_Id = uuid.uuid4().hex,
        Batch_Id = "Batch_Id",
        Validation_Processing_Stage = "Validation_Processing_Stage",
        Rule_Cd = 'Empty row pattern check',
        Error_Desc = ", ".join(e.args),
        Submission_Period_Cd = "Submission_Period_Cd",
        Process_Cd = "Process_Cd",
        Template_Version = "Template_Version",
        Organisation_Cd = "Organisation_Cd",
    )

except Exception as e:
    raise RuntimeError(f"Unexpected exception during processing: {e}")
```

> ✅ If successful, `pivoted_df` contains the loaded and validated data.  
> ❌ If validation fails, `error_df` will contain a structured error record for review or logging.



### 12. Boncode-Description Consistency

After loading the data, additional validation checks can be performed using pandas, which offers more flexibility than Excel for these types of operations. Specifically, we verify that Boncodes do not have multiple descriptions, and that the same description does not correspond to multiple Boncodes.

> Note: The output has been updated to report errors on a per-row basis, making it easier to identify and address individual issues.

```python
same_description_different_boncodes = dqchecks.panacea.create_same_desc_diff_boncode_validation_event(pivoted_df, metadata)
same_measure_diff_description = dqchecks.panacea.create_same_boncode_diff_desc_validation_event(pivoted_df, metadata)
```

#### Sample output


| Event_Id | Batch_Id                            | Validation_Processing_Stage  | Sheet_Cd | Template_Version                         | Rule_Cd                                | Organisation_Cd | Measure_Cd | Measure_Unit | Measure_Desc | Submission_Period_Cd | Process_Cd | Error_Category                    | Section_Cd | Cell_Cd | Data_Column | Error_Value | Error_Severity_Cd | Error_Desc                                                  |
|----------|-------------------------------------|-------------------------------|----------|-------------------------------------------|-----------------------------------------|------------------|------------|---------------|---------------|------------------------|------------|----------------------------------|-------------|---------|--------------|--------------|-------------------|---------------------------------------------------------------|
| 0        | 24efed9df6004e60ba48c0f75fa4dea0     | Template-based validations   | None     | template_after_python_errata_changes.xlsx | Rule 1 - Boncode-Description Consistency | template         | None       | None          | None          | 2025-07               | apr        | Same description, different boncodes | None        | None    | None         | None         | soft              | Measure_Desc 'Other income - Statutory' used with multiple boncodes |
| 1        | 5c97eace3f2444d0a43b308e5121c039 | Template-based validations    | None     | template_after_python_errata_changes.xlsx | Rule 1 - Boncode-Description Consistency | template        | None       | None         | None         | 2025-07              | apr        | Same boncode, different description | None       | None    | None        | None        | soft              | Measure_Cd 'B0372TEO_F' used with multiple Measure_Desc values |



### 13. Mandatory Field Validation

Verifies that key fields in the flat table — specifically `"Reference"`, `"Item Description"`, and `"Unit"` — are not null or empty. Each row missing one or more of these fields will result in a validation event.

```python
dqchecks.panacea.create_nulls_in_measure_validation_event(pivoted_df, metadata)
```

#### Sample output
| Event_Id | Batch_Id                            | Validation_Processing_Stage  | Sheet_Cd | Template_Version                         | Rule_Cd                                | Organisation_Cd | Measure_Cd | Measure_Unit | Measure_Desc | Submission_Period_Cd | Process_Cd | Error_Category                    | Section_Cd | Cell_Cd | Data_Column | Error_Value | Error_Severity_Cd | Error_Desc                                                  |
|----------|-------------------------------------|-------------------------------|----------|-------------------------------------------|-----------------------------------------|------------------|------------|---------------|---------------|------------------------|------------|----------------------------------|-------------|---------|--------------|--------------|-------------------|---------------------------------------------------------------|
|0|1|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|1,2,3|


### 14. Process-Model Mapping Validation

This check ensures that the `Model` column in the `"F_Output"` sheet contains the correct model name based on the `Process_Cd` value. The expected mappings are:

- For `APR`, the model must be `"Cyclical Foundation"`
- For `BPT`, the model must be `"Price Review 2024"`
- For `PCD`, the model must be `"Delta"`

Each row in the dataset is validated against this mapping.

```python
dqchecks.panacea.create_process_model_mapping_validation_event(
    pivoted_df,
    dqchecks.panacea.PROCESS_MODEL_MAPPING,
    metadata
)
```

#### Sample output
| Event_Id | Batch_Id                            | Validation_Processing_Stage  | Sheet_Cd | Template_Version                         | Rule_Cd                                | Organisation_Cd | Measure_Cd | Measure_Unit | Measure_Desc | Submission_Period_Cd | Process_Cd | Error_Category                    | Section_Cd | Cell_Cd | Data_Column | Error_Value | Error_Severity_Cd | Error_Desc                                                  |
|----------|-------------------------------------|-------------------------------|----------|-------------------------------------------|-----------------------------------------|------------------|------------|---------------|---------------|------------------------|------------|----------------------------------|-------------|---------|--------------|--------------|-------------------|---------------------------------------------------------------|
|0|1|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|