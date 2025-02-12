import pytest
import pandas as pd
import datetime
from io import BytesIO

from dqchecks.transforms import process_fout_sheets

# A helper function to simulate loading an Excel file with Pandas
def create_excel_file(sheet_data):
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        for sheet_name, data in sheet_data.items():
            data.to_excel(writer, sheet_name=sheet_name, index=False)
    excel_buffer.seek(0)
    return pd.ExcelFile(excel_buffer)


# Sample data for testing
@pytest.fixture
def valid_excel_data():
    # Create a sample DataFrame
    data = {
        "1": ["Acronym", "", "a", "b", "c"],
        "2": ["Reference", "", "a", "b", "c"],
        "3": ["Item description", "", "a", "b", "c"],
        "4": ["Unit", "", "a", "b", "c"],
        "5": ["Model", "", "a", "b", "c"],
        "6": ["2020-21", "", "a", "b", "c"],
        "7": ["2021-22", "", "a", "b", "c"],
        "8": ["2022-23", "", "a", "b", "c"],
    }

    # Create the DataFrame
    df = pd.DataFrame(data)
    return {
        'fOut_2023': df,
        'fOut_2024': df
    }


# Test case 1: Valid input
def test_valid_input(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    result_df = process_fout_sheets(
        xlfile, org_cd, submission_period_cd, process_cd, template_version, last_modified)

    # Check if the result DataFrame has the expected columns
    expected_columns = [
        'Organisation_Cd', 'Submission_Period_Cd', 'Observation_Period_Cd', 'Process_Cd',
        'Template_Version', 'Sheet_Cd', 'Measure_Cd', 'Measure_Value', 'Measure_Desc', 'Measure_Unit',
        'Model_Cd', 'Submission_Date'
    ]

    assert set(result_df.columns) == set(expected_columns)
    assert not result_df.empty


# Test case 2: Invalid xlfile type (not a pd.ExcelFile)
def test_invalid_xlfile_type(valid_excel_data):
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(TypeError):
        process_fout_sheets(
            "invalid_excel_file", org_cd, submission_period_cd, process_cd, template_version, last_modified)


# Test case 3: Missing org_cd
def test_missing_org_cd(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(ValueError):
        process_fout_sheets(
            xlfile, "", submission_period_cd, process_cd, template_version, last_modified)


# Test case 4: No fOut_ sheets
def test_no_fout_sheets():
    sheet_data = {
        'OtherSheet': pd.DataFrame({
            "Reference": [1],
            "Item description": ["Item 1"],
            "Unit": ["kg"],
            "Model": ["A"],
            "2020-21": [10],
        })
    }
    xlfile = create_excel_file(sheet_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(Exception, match="No fOut_*"):
        process_fout_sheets(xlfile, org_cd, submission_period_cd, process_cd, template_version, last_modified)

# Test case 5: Empty sheet (no data after dropping NaN rows)
def test_empty_sheet():
    sheet_data = {
        'fOut_Empty': pd.DataFrame({
            "Reference": ["Reference", None, None, None],
            "Item description": ["Item description", None, None, None],
            "Unit": ["Unit", None, None, None],
            "Model": ["Model", None, None, None],
            "2020-21": ["2020-21", None, None, None],
        })
    }
    xlfile = create_excel_file(sheet_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(Exception, match="An error occurred while processing the Excel sheets: No valid data found after removing rows with NaN values."):
        process_fout_sheets(xlfile, org_cd, submission_period_cd, process_cd, template_version, last_modified)


# Test case 6: Missing observation period columns
def test_missing_observation_columns(valid_excel_data):
    # Modify the sample data to not contain any observation period columns (like '2020-21')
    data = {
        "Reference": [1, 2, 3],
        "Item description": ["Item 1", "Item 2", "Item 3"],
        "Unit": ["kg", "g", "lbs"],
        "Model": ["A", "B", "C"],
    }
    df = pd.DataFrame(data)
    sheet_data = {
        'fOut_2023': df
    }
    xlfile = create_excel_file(sheet_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(Exception, match="An error occurred while processing the Excel sheets: No observation period columns found in the data."):
        process_fout_sheets(xlfile, org_cd, submission_period_cd, process_cd, template_version, last_modified)


# Test case 7: Correct data types in output
def test_output_data_types(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    result_df = process_fout_sheets(
        xlfile, org_cd, submission_period_cd, process_cd, template_version, last_modified)

    # Ensure all columns are of type string as per the function's behavior
    assert all(result_df[column].dtype == 'object' for column in result_df.columns)

# Test case 8: Missing submission_period_cd
def test_missing_submission_period_cd(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    org_cd = "ORG123"
    process_cd = "P01"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(ValueError):
        process_fout_sheets(
            xlfile, org_cd, "", process_cd, template_version, last_modified)

# Test case 9: Missing process_cd
def test_missing_submission_process_cd(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    template_version = "V1"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(ValueError):
        process_fout_sheets(
            xlfile, org_cd, submission_period_cd, "", template_version, last_modified)

# Test case 10: Missing template_version
def test_missing_submission_template_version(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    last_modified = datetime.datetime(2025, 2, 11)

    with pytest.raises(ValueError):
        process_fout_sheets(
            xlfile, org_cd, submission_period_cd, process_cd, "", last_modified)
        
# Test case 11: Missing last_modified
def test_missing_submission_last_modified(valid_excel_data):
    xlfile = create_excel_file(valid_excel_data)
    org_cd = "ORG123"
    submission_period_cd = "2025Q1"
    process_cd = "P01"
    template_version = "V1"

    with pytest.raises(ValueError):
        process_fout_sheets(
            xlfile, org_cd, submission_period_cd, process_cd, template_version, None)

if __name__ == '__main__':
    pytest.main()
