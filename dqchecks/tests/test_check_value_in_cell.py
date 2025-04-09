"""
Test create_dataframe_from_company_selection_check
function from panacea.py file 
"""
import pytest
from openpyxl import Workbook
import pandas as pd
from dqchecks.panacea import (
    check_value_in_cell,
    create_dataframe_from_company_selection_check
)

@pytest.fixture
def sample_workbook() -> Workbook:
    """
    Creates a sample workbook with data for testing purposes.
    """
    wb = Workbook()
    sheet = wb.active
    sheet.title = "Sheet1"

    # Add some data to the sheet for testing
    sheet["B5"] = "TestValue"
    sheet["B6"] = "AnotherValue"
    return wb

# pylint: disable=W0621
def test_check_value_in_cell_value_found(sample_workbook: Workbook):
    """
    Test case for checking when the value matches the value in the specified cell.
    """
    result = check_value_in_cell(sample_workbook, "Sheet1", "TestValue", "B5")

    assert result["status"] == "Ok"
    assert result["description"] == "Value found in cell"
    assert not result["errors"]
    assert result["meta"]["sheet_name"] == "Sheet1"
    assert result["meta"]["cell_name"] == "B5"


def test_check_value_in_cell_value_mismatch(sample_workbook: Workbook):
    """
    Test case for checking when the value does not match the value in the specified cell.
    """
    result = check_value_in_cell(sample_workbook, "Sheet1", "MismatchValue", "B5")

    assert result["status"] == "Error"
    assert result["description"] == "Value mismatch"
    assert result["errors"] == ["Expected [MismatchValue] found [TestValue]"]
    assert result["meta"]["sheet_name"] == "Sheet1"
    assert result["meta"]["cell_name"] == "B5"


def test_check_value_in_cell_missing_sheet(sample_workbook: Workbook):
    """
    Test case for checking when the specified sheet is not found in the workbook.
    """
    result = check_value_in_cell(sample_workbook, "NonExistentSheet", "TestValue", "B5")

    assert result["status"] == "Error"
    assert result["description"] == "Missing sheet"
    assert result["errors"] == ["Sheet 'NonExistentSheet' not found in the workbook."]
    assert result["meta"]["sheet_name"] == "NonExistentSheet"
    assert result["meta"]["cell_name"] == "B5"


def test_check_value_in_cell_invalid_cell_name(sample_workbook: Workbook):
    """
    Test case for checking when the provided cell name is invalid.
    """
    with pytest.raises(ValueError) as exc_info:
        check_value_in_cell(sample_workbook, "Sheet1", "TestValue", "InvalidCell")

    assert str(exc_info.value) == "Invalid cell name 'InvalidCell' in sheet 'Sheet1'."


def test_check_value_in_cell_invalid_workbook_type():
    """
    Test case for checking when the 'workbook' argument is not of the correct type.
    """
    with pytest.raises(ValueError) as exc_info:
        check_value_in_cell("NotAWorkbook", "Sheet1", "TestValue", "B5")

    assert (
        str(exc_info.value) ==
        "The 'workbook' argument must be a valid openpyxl Workbook object."
    )


def test_check_value_in_cell_invalid_sheet_name(sample_workbook: Workbook):
    """
    Test case for checking when the 'sheet_name' argument is not a string.
    """
    with pytest.raises(ValueError) as exc_info:
        check_value_in_cell(sample_workbook, 123, "TestValue", "B5")

    assert str(exc_info.value) == "The 'sheet_name' argument must be a non-empty string."


def test_check_value_in_cell_invalid_value_type(sample_workbook: Workbook):
    """
    Test case for checking when the 'value' argument is not a valid type (e.g., list).
    """
    with pytest.raises(ValueError) as exc_info:
        check_value_in_cell(sample_workbook, "Sheet1", [], "B5")

    assert (
        str(exc_info.value) ==
        "The 'value' argument must be a string, integer, float, or boolean.")


def test_check_value_in_cell_invalid_cell_name_type(sample_workbook: Workbook):
    """
    Test case for checking when the 'cell_name' argument is not a string.
    """
    with pytest.raises(ValueError) as exc_info:
        check_value_in_cell(sample_workbook, "Sheet1", "TestValue", 5)

    assert (
        str(exc_info.value) ==
        "The 'cell_name' argument must be a non-empty string (e.g., 'B5').")

def test_create_dataframe_valid_data():
    """
    Test case for valid input where errors are provided and metadata is correctly formed.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "errors": ["Company name mismatch in B5", "Company name mismatch in B6"],
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    # Call the function
    df = create_dataframe_from_company_selection_check(input_data)

    # Assert that the DataFrame is created correctly
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 7)  # Two errors, 7 columns
    assert (
        set(df.columns) ==
        {"Event_Id",
         "Sheet_Cd",
         "Cell_Cd",
         "Rule_Cd",
         "Error_Category",
         "Error_Severity_Cd",
         "Error_Desc"})

    # Check if the values in the dataframe match the input data
    assert df.iloc[0]["Sheet_Cd"] == "Sheet1"
    assert df.iloc[0]["Cell_Cd"] == "B5"
    assert df.iloc[0]["Error_Desc"] == "Company name mismatch in B5"
    assert df.iloc[1]["Error_Desc"] == "Company name mismatch in B6"
    assert df.iloc[0]["Rule_Cd"] == "Rule 7: Company Name Selected"
    assert df.iloc[0]["Error_Category"] == "Company name mismatch"
    assert df.iloc[0]["Error_Severity_Cd"] == "?"

def test_create_dataframe_no_errors():
    """
    Test case for input where no errors are provided. This should return an empty DataFrame.
    """
    input_data = {
        "status": "Ok",
        "description": "Value matched",
        "errors": [],
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    # Call the function
    df = create_dataframe_from_company_selection_check(input_data)

    # Assert that the DataFrame is empty but has the correct columns
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (0, 7)  # No errors, 7 columns
    assert (
        set(df.columns) ==
        {
            "Event_Id",
            "Sheet_Cd",
            "Cell_Cd",
            "Rule_Cd",
            "Error_Category",
            "Error_Severity_Cd",
            "Error_Desc"})

def test_create_dataframe_invalid_input_data():
    """
    Test case for invalid input where 'input_data' is not a dictionary.
    """
    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_selection_check("invalid input")

    assert str(exc_info.value) == "The 'input_data' argument must be a dictionary."

def test_create_dataframe_missing_errors_key():
    """
    Test case for input where the 'errors' key is missing from the input data.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_selection_check(input_data)

    assert str(exc_info.value) == "The 'input_data' must contain the 'errors' key."

def test_create_dataframe_invalid_errors_type():
    """
    Test case for invalid 'errors' key where the value is not a list.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "errors": "This should be a list",
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_selection_check(input_data)

    assert str(exc_info.value) == "The 'errors' key must be a list."

def test_create_dataframe_missing_meta_key():
    """
    Test case for input where the 'meta' key is missing from the input data.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "errors": ["Company name mismatch in B5"],
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_selection_check(input_data)

    assert str(exc_info.value) == "The 'input_data' must contain the 'meta' key."

def test_create_dataframe_invalid_meta_structure():
    """
    Test case for input where 'meta' does not contain the required 'sheet_name' and 'cell_name'.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "errors": ["Company name mismatch in B5"],
        "meta": {
            "sheet_name": "Sheet1"
            # Missing 'cell_name'
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_selection_check(input_data)

    assert (
        str(exc_info.value) ==
        "The 'meta' key must be a dictionary containing 'sheet_name' and 'cell_name'.")

def test_create_dataframe_missing_sheet_name_in_meta():
    """
    Test case for input where 'meta' contains an invalid structure with no 'sheet_name'.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "errors": ["Company name mismatch in B5"],
        "meta": {
            "cell_name": "B5"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_selection_check(input_data)

    assert (
        str(exc_info.value) ==
        "The 'meta' key must be a dictionary containing 'sheet_name' and 'cell_name'.")

def test_create_dataframe_with_uuid():
    """
    Test case to check if the 'Event_Id' column contains unique UUIDs for each row.
    """
    input_data = {
        "status": "Error",
        "description": "Value mismatch",
        "errors": ["Company name mismatch in B5"],
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    df = create_dataframe_from_company_selection_check(input_data)

    # Assert that the Event_Id column contains UUIDs
    assert len(df["Event_Id"]) == 1
    # UUID should be a string
    assert isinstance(df["Event_Id"].iloc[0], str)
    # UUID length should be 32 characters (without hyphens)
    assert len(df["Event_Id"].iloc[0]) == 32
    # UUID is lowercase alphanumeric
    assert all(i in 'abcdefghijklmopqrstuvwxyz0123456789' for i in df["Event_Id"].iloc[0])
