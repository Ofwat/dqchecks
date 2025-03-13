"""
Testing create_dataframe_missing_sheets function
from panacea.py file
"""
import pytest
import pandas as pd
from dqchecks.panacea import create_dataframe_missing_sheets, MissingSheetContext

def test_create_dataframe_missing_sheets_valid():
    """Test the function with valid inputs."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    input_data = {
        'errors': {
            'Missing In Spreadsheet 2': ['Sheet1', 'Sheet2']
        }
    }
    df = create_dataframe_missing_sheets(input_data, context)

    # Check if the dataframe is created and has the correct number of rows
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Two sheets in the input data
    assert 'Event_Id' in df.columns
    assert 'Sheet_Cd' in df.columns
    assert 'Rule_Cd' in df.columns
    assert 'Error_Category' in df.columns
    assert 'Error_Severity_Cd' in df.columns
    assert 'Error_Desc' in df.columns

    # Check the content of the rows
    assert df['Sheet_Cd'].iloc[0] == 'Sheet1'
    assert df['Sheet_Cd'].iloc[1] == 'Sheet2'

def test_create_dataframe_missing_sheets_invalid_input_data_type():
    """Test the function when 'input_data' is not a dictionary."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    with pytest.raises(ValueError, match="The 'input_data' argument must be a dictionary."):
        create_dataframe_missing_sheets([], context)

def test_create_dataframe_missing_sheets_invalid_context_type():
    """Test the function when 'context' is not of type MissingSheetContext."""
    input_data = {
        'errors': {
            'Missing In Spreadsheet 2': ['Sheet1', 'Sheet2']
        }
    }
    with pytest.raises(ValueError,
            match="The 'context' argument must be of type MissingSheetContext."):
        create_dataframe_missing_sheets(input_data, {})

def test_create_dataframe_missing_sheets_invalid_missing_sheets_format():
    """Test the function when 'Missing In Spreadsheet 2' is not a list."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    input_data = {
        'errors': {
            'Missing In Spreadsheet 2': "Invalid format"
        }
    }
    df = create_dataframe_missing_sheets(input_data, context)
    assert len(df) == 0  # Since the format is invalid, the result should be an empty DataFrame

def test_create_dataframe_missing_sheets_invalid_sheet_name():
    """Test the function when one of the sheets is invalid (e.g., empty string)."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    input_data = {
        'errors': {
            'Missing In Spreadsheet 2': ['Sheet1', '', 'Sheet2']
        }
    }
    with pytest.raises(ValueError,
            match="Invalid sheet name: ''. Each sheet must be a non-empty string."):
        create_dataframe_missing_sheets(input_data, context)

def test_create_dataframe_missing_sheets_no_missing_sheets():
    """Test the function when there are no missing sheets in the input data."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    input_data = {
        'errors': {
            'Missing In Spreadsheet 2': []
        }
    }
    df = create_dataframe_missing_sheets(input_data, context)
    # The result should be an empty DataFrame since no missing sheets are provided
    assert len(df) == 0

def test_create_dataframe_missing_sheets_fallback_to_empty_list():
    """Test the function when 'Missing In Spreadsheet 2' is not in the 'errors' key."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    input_data = {
        'errors': {}
    }
    df = create_dataframe_missing_sheets(input_data, context)
    assert len(df) == 0  # The result should be an empty DataFrame since there are no missing sheets
