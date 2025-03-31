"""
Tests for create_missing_sheet_row function from panacea.py
"""
import pytest
from dqchecks.panacea import (
    create_missing_sheet_row,
    MissingSheetContext,
    MissingSheetRow,)


# Test Cases
def test_create_missing_sheet_row_valid():
    """Test the function with valid inputs."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    sheet = "Sheet1"
    result = create_missing_sheet_row(sheet, context)

    assert result.Sheet_Cd == sheet
    assert result.Rule_Cd == context.Rule_Cd
    assert result.Error_Category == context.Error_Category
    assert result.Error_Severity_Cd == context.Error_Severity_Cd
    assert result.Error_Desc == "Missing Sheet"
    # Check that Event_Id is a valid UUID hex string
    assert len(result.Event_Id) == 32

def test_create_missing_sheet_row_invalid_sheet_type():
    """Test the function when an invalid 'sheet' type is passed (not a string)."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")

    with pytest.raises(ValueError,
            match="The 'sheet' argument must be a non-empty string."):
        create_missing_sheet_row(123, context)  # Passing an integer instead of a string

    with pytest.raises(ValueError,
            match="The 'sheet' argument must be a non-empty string."):
        create_missing_sheet_row("", context)  # Passing an empty string

def test_create_missing_sheet_row_invalid_context_type():
    """Test the function when an invalid 'context'
    type is passed (not an instance of MissingSheetContext)."""
    with pytest.raises(ValueError,
            match="The 'context' argument must be of type MissingSheetContext."):
        # Passing a dictionary instead of a MissingSheetContext
        create_missing_sheet_row("Sheet1", {})

def test_create_missing_sheet_row_invalid_rule_cd():
    """Test the function when an invalid 'Rule_Cd' is passed in the context."""
    invalid_context = MissingSheetContext(
        Rule_Cd="",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    with pytest.raises(ValueError,
            match="Invalid context: Invalid 'Rule_Cd': it must be a non-empty string."):
        create_missing_sheet_row("Sheet1", invalid_context)  # Rule_Cd is an empty string

def test_create_missing_sheet_row_invalid_error_category():
    """Test the function when an invalid 'Error_Category' is passed in the context."""
    invalid_context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="",
        Error_Severity_Cd="soft")
    with pytest.raises(ValueError,
            match="Invalid context: Invalid 'Error_Category': it must be a non-empty string."):
        create_missing_sheet_row("Sheet1", invalid_context)  # Error_Category is an empty string

def test_create_missing_sheet_row_invalid_error_severity_cd():
    """Test the function when an invalid 'Error_Severity_Cd' is passed in the context."""
    invalid_context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="")
    with pytest.raises(ValueError,
            match="Invalid context: Invalid 'Error_Severity_Cd': it must be a non-empty string."):
        # Error_Severity_Cd is an empty string
        create_missing_sheet_row("Sheet1", invalid_context)

# Test data for valid and invalid cases
valid_data = {
    'Event_Id': '12345',
    'Sheet_Cd': 'Sheet1',
    'Rule_Cd': 'Rule1',
    'Error_Category': 'Data',
    'Error_Severity_Cd': 'High',
    'Error_Desc': 'Missing Sheet'
}

invalid_data = {
    'Event_Id': '',
    'Sheet_Cd': '',
    'Rule_Cd': '',
    'Error_Category': '',
    'Error_Severity_Cd': '',
    'Error_Desc': ''
}

def test_named_tuple_initialization():
    """Test the constructor and initialization of the NamedTuple"""
    row = MissingSheetRow(**valid_data)

    # Ensure the row is initialized correctly
    assert row.Event_Id == valid_data['Event_Id']
    assert row.Sheet_Cd == valid_data['Sheet_Cd']
    assert row.Rule_Cd == valid_data['Rule_Cd']
    assert row.Error_Category == valid_data['Error_Category']
    assert row.Error_Severity_Cd == valid_data['Error_Severity_Cd']
    assert row.Error_Desc == valid_data['Error_Desc']

def test_valid_data_validation():
    """Test the validation method for correct input"""
    row = MissingSheetRow(**valid_data)

    # Should not raise any exceptions for valid data
    row.validate()

# Test the validation method for invalid input
@pytest.mark.parametrize(
    'field, invalid_value',
    [
        ('Event_Id', ''),
        ('Sheet_Cd', ''),
        ('Rule_Cd', ''),
        ('Error_Category', ''),
        ('Error_Severity_Cd', ''),
        ('Error_Desc', '')
    ]
)
def test_invalid_data_validation(field: str, invalid_value: str):
    """Modify the valid data to create an invalid instance"""
    invalid_data_copy = valid_data.copy()
    invalid_data_copy[field] = invalid_value

    row = MissingSheetRow(**invalid_data_copy)

    # Validate and ensure a ValueError is raised
    with pytest.raises(ValueError):
        row.validate()

def test_to_dict():
    """Test the `to_dict` method for correct output"""
    row = MissingSheetRow(**valid_data)

    # Get the dictionary representation of the row
    row_dict = row.to_dict()

    # Ensure the dictionary matches the expected structure
    assert row_dict == {
        'Event_Id': valid_data['Event_Id'],
        'Sheet_Cd': valid_data['Sheet_Cd'],
        'Rule_Cd': valid_data['Rule_Cd'],
        'Error_Category': valid_data['Error_Category'],
        'Error_Severity_Cd': valid_data['Error_Severity_Cd'],
        'Error_Desc': valid_data['Error_Desc']
    }

def test_to_dict_invalid_data():
    """Test the to_dict method when data is invalid (check for missing keys)"""
    row = MissingSheetRow(**valid_data)

    # Modify the row to make data invalid
    invalid_row = row._replace(Sheet_Cd='')  # This should trigger an error on validation

    # Ensure that calling `to_dict` does not throw an exception on invalid data
    row_dict = invalid_row.to_dict()

    # Check the data is converted to dict even with invalid input
    assert row_dict == {
        'Event_Id': valid_data['Event_Id'],
        'Sheet_Cd': '',  # Empty string, which is technically still a valid field
        'Rule_Cd': valid_data['Rule_Cd'],
        'Error_Category': valid_data['Error_Category'],
        'Error_Severity_Cd': valid_data['Error_Severity_Cd'],
        'Error_Desc': valid_data['Error_Desc']
    }
