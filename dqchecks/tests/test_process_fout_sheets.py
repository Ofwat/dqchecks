"""
Tests for the transforms.py file
"""
import datetime
from io import BytesIO
import pytest
import pandas as pd

from dqchecks.transforms import process_fout_sheets, ProcessingContext


def create_excel_file(sheet_data):
    """
    A helper function to simulate loading an Excel file with Pandas.

    Args:
        sheet_data (dict): A dictionary where keys are sheet names and values are DataFrames.

    Returns:
        pd.ExcelFile: A pandas ExcelFile object containing the sheets in the provided data.
    """
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        for sheet_name, data in sheet_data.items():
            data.to_excel(writer, sheet_name=sheet_name, index=False)
    excel_buffer.seek(0)
    return pd.ExcelFile(excel_buffer)


def valid_excel_data():
    """
    Fixture that creates a sample DataFrame for testing purposes.

    Returns:
        dict: A dictionary with sheet names as keys and DataFrames as values.
    """
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
    df = pd.DataFrame(data)
    return {
        'fOut_2023': df,
        'fOut_2024': df
    }


def test_valid_input():
    """
    Test case to check the function's behavior with valid input data.

    The test verifies that the resulting DataFrame contains the expected columns
    and is not empty.
    """
    xlfile = create_excel_file(valid_excel_data())

    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    result_df = process_fout_sheets(xlfile, context)

    expected_columns = [
        'Organisation_Cd', 'Submission_Period_Cd', 'Observation_Period_Cd', 'Process_Cd',
        'Template_Version', 'Sheet_Cd', 'Measure_Cd', 'Measure_Value', 'Measure_Desc',
        'Measure_Unit', 'Model_Cd', 'Submission_Date'
    ]

    assert set(result_df.columns) == set(expected_columns)
    assert not result_df.empty


def test_invalid_xlfile_type():
    """
    Test case to verify that an error is raised when the provided file is not an Excel file.

    This test checks if the function raises a TypeError when the input is not an
    instance of pd.ExcelFile.
    """
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(TypeError):
        process_fout_sheets("invalid_excel_file", context)


def test_missing_org_cd():
    """
    Test case to check if a ValueError is raised when 'org_cd' is missing or empty.

    The function verifies that a ValueError is raised if the organization code is not provided.
    """
    xlfile = create_excel_file(valid_excel_data())
    context = ProcessingContext(
        org_cd="",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(ValueError):
        process_fout_sheets(xlfile, context)


def test_no_fout_sheets():
    """
    Test case to check if the function raises an exception when there are no fOut_* sheets.

    The test verifies that an exception is raised if the Excel file does not contain any sheet
    with the name starting with 'fOut_'.
    """
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
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(Exception, match="No fOut_*"):
        process_fout_sheets(xlfile, context)


def test_empty_sheet():
    """
    Test case to verify if the function raises a ValueError when a sheet contains no valid data.

    The function checks if a ValueError is raised when all rows of the sheet are NaN after dropping.
    """
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
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(ValueError,
            match="No valid data found after removing rows with NaN values."):
        process_fout_sheets(xlfile, context)


def test_missing_observation_columns():
    """
    Test case to check if a ValueError is raised when the observation period columns are missing.

    This function checks that the function raises an error if no columns for observation periods
    (like '2020-21') are found in the data.
    """
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
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(ValueError, match="No observation period columns found in the data."):
        process_fout_sheets(xlfile, context)


def test_output_data_types():
    """
    Test case to verify if the output DataFrame contains the correct data types.

    This test ensures that all columns in the resulting DataFrame are of type 'object' (string).
    """
    xlfile = create_excel_file(valid_excel_data())
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    result_df = process_fout_sheets(xlfile, context)

    assert all(result_df[column].dtype == 'object' for column in result_df.columns)


def test_missing_submission_period_cd():
    """
    Test case to check if a ValueError is raised when 'submission_period_cd' is missing or empty.

    The function verifies that a ValueError is raised if the submission period code is not provided.
    """
    xlfile = create_excel_file(valid_excel_data())
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(ValueError):
        process_fout_sheets(xlfile, context)


def test_missing_submission_process_cd():
    """
    Test case to check if a ValueError is raised when 'process_cd' is missing or empty.

    The function verifies that a ValueError is raised if the process code is not provided.
    """
    xlfile = create_excel_file(valid_excel_data())
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="",
        template_version="v1.0",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(ValueError):
        process_fout_sheets(xlfile, context)


def test_missing_submission_template_version():
    """
    Test case to check if a ValueError is raised when 'template_version' is missing or empty.

    The function verifies that a ValueError is raised if the template version is not provided.
    """
    xlfile = create_excel_file(valid_excel_data())
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="",
        last_modified=datetime.datetime(2025, 2, 11)
    )

    with pytest.raises(ValueError):
        process_fout_sheets(xlfile, context)


def test_missing_submission_last_modified():
    """
    Test case to check if a ValueError is raised when 'last_modified' is missing or None.

    The function checks that a ValueError is raised if the last modified timestamp isn't provided.
    """
    xlfile = create_excel_file(valid_excel_data())
    context = ProcessingContext(
        org_cd="ORG123",
        submission_period_cd="2025Q1",
        process_cd="process_1",
        template_version="v1.0",
        last_modified=None
    )

    with pytest.raises(ValueError):
        process_fout_sheets(xlfile, context)


if __name__ == '__main__':
    pytest.main()
