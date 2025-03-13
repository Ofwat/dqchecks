"""
Tests for create_missing_sheet_row function from panacea.py
"""
import pytest
from dqchecks.panacea import create_missing_sheet_row, MissingSheetContext


# Test Cases
def test_create_missing_sheet_row_valid():
    """Test the function with valid inputs."""
    context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="soft")
    sheet = "Sheet1"
    result = create_missing_sheet_row(sheet, context)

    # Check if all expected keys are in the result dictionary
    assert 'Event_Id' in result
    assert result['Sheet_Cd'] == sheet
    assert result['Rule_Cd'] == context.Rule_Cd
    assert result['Error_Category'] == context.Error_Category
    assert result['Error_Severity_Cd'] == context.Error_Severity_Cd
    assert result['Error_Desc'] == "Missing Sheet"
    # Check that Event_Id is a valid UUID hex string
    assert len(result['Event_Id']) == 32

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
            match="Invalid 'Rule_Cd' in context: it must be a non-empty string."):
        create_missing_sheet_row("Sheet1", invalid_context)  # Rule_Cd is an empty string

def test_create_missing_sheet_row_invalid_error_category():
    """Test the function when an invalid 'Error_Category' is passed in the context."""
    invalid_context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="",
        Error_Severity_Cd="soft")
    with pytest.raises(ValueError,
            match="Invalid 'Error_Category' in context: it must be a non-empty string."):
        create_missing_sheet_row("Sheet1", invalid_context)  # Error_Category is an empty string

def test_create_missing_sheet_row_invalid_error_severity_cd():
    """Test the function when an invalid 'Error_Severity_Cd' is passed in the context."""
    invalid_context = MissingSheetContext(
        Rule_Cd="?",
        Error_Category="Missing Sheet",
        Error_Severity_Cd="")
    with pytest.raises(ValueError,
            match="Invalid 'Error_Severity_Cd' in context: it must be a non-empty string."):
        # Error_Severity_Cd is an empty string
        create_missing_sheet_row("Sheet1", invalid_context)
