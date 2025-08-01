"""
Test create_dataframe_from_company_acronym_check
function from panacea.py file
"""
import pytest
import pandas as pd
from dqchecks.panacea import create_dataframe_from_company_acronym_check

def test_create_dataframe_acronym_valid_data():
    """
    Test case for valid input where errors are provided and metadata is correctly formed.
    """
    input_data = {
        "status": "Error",
        "description": "Acronym mismatch",
        "errors": ["Acronym mismatch in B5", "Acronym mismatch in B6"],
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    df = create_dataframe_from_company_acronym_check(input_data)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 7)
    assert set(df.columns) == {
        "Event_Id", "Sheet_Cd", "Cell_Cd", "Rule_Cd",
        "Error_Category", "Error_Severity_Cd", "Error_Desc"
    }

    assert df.iloc[0]["Sheet_Cd"] == "Sheet1"
    assert df.iloc[0]["Cell_Cd"] == "B5"
    assert df.iloc[0]["Error_Desc"] == "Acronym mismatch in B5"
    assert df.iloc[1]["Error_Desc"] == "Acronym mismatch in B6"
    assert df.iloc[0]["Rule_Cd"] == "Rule 8: Company Acronym Check"
    assert df.iloc[0]["Error_Category"] == "Company acronym mismatch"
    assert df.iloc[0]["Error_Severity_Cd"] == "?"


def test_create_dataframe_acronym_no_errors():
    """
    Test case where input has no errors; should return an empty DataFrame.
    """
    input_data = {
        "status": "Ok",
        "description": "Acronym matched",
        "errors": [],
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    df = create_dataframe_from_company_acronym_check(input_data)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (0, 7)
    assert set(df.columns) == {
        "Event_Id", "Sheet_Cd", "Cell_Cd", "Rule_Cd",
        "Error_Category", "Error_Severity_Cd", "Error_Desc"
    }


def test_create_dataframe_acronym_invalid_input_data():
    """
    Test case where input_data is not a dictionary.
    """
    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_acronym_check("invalid input")

    assert str(exc_info.value) == "The 'input_data' argument must be a dictionary."


def test_create_dataframe_acronym_missing_errors_key():
    """
    Test case where 'errors' key is missing.
    """
    input_data = {
        "status": "Error",
        "description": "Missing errors key",
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_acronym_check(input_data)

    assert str(exc_info.value) == "The 'input_data' must contain the 'errors' key."


def test_create_dataframe_acronym_invalid_errors_type():
    """
    Test case where 'errors' is not a list.
    """
    input_data = {
        "status": "Error",
        "description": "Invalid errors type",
        "errors": "This should be a list",
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_acronym_check(input_data)

    assert str(exc_info.value) == "The 'errors' key must be a list."


def test_create_dataframe_acronym_missing_meta_key():
    """
    Test case where 'meta' key is missing.
    """
    input_data = {
        "status": "Error",
        "description": "Missing meta key",
        "errors": ["Error 1"]
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_acronym_check(input_data)

    assert str(exc_info.value) == "The 'input_data' must contain the 'meta' key."


def test_create_dataframe_acronym_invalid_meta_structure():
    """
    Test case where 'meta' is missing required keys.
    """
    input_data = {
        "status": "Error",
        "description": "Invalid meta structure",
        "errors": ["Mismatch"],
        "meta": {
            "sheet_name": "Sheet1"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_acronym_check(input_data)

    assert (
        str(exc_info.value) ==
        "The 'meta' key must be a dictionary containing 'sheet_name' and 'cell_name'."
    )


def test_create_dataframe_acronym_missing_sheet_name_in_meta():
    """
    Test case where 'sheet_name' is missing in meta.
    """
    input_data = {
        "status": "Error",
        "description": "Missing sheet_name",
        "errors": ["Mismatch"],
        "meta": {
            "cell_name": "B5"
        }
    }

    with pytest.raises(ValueError) as exc_info:
        create_dataframe_from_company_acronym_check(input_data)

    assert (
        str(exc_info.value) ==
        "The 'meta' key must be a dictionary containing 'sheet_name' and 'cell_name'."
    )


def test_create_dataframe_acronym_with_uuid():
    """
    Test that each row has a unique Event_Id in UUID format.
    """
    input_data = {
        "status": "Error",
        "description": "Acronym mismatch",
        "errors": ["Mismatch in B5"],
        "meta": {
            "sheet_name": "Sheet1",
            "cell_name": "B5"
        }
    }

    df = create_dataframe_from_company_acronym_check(input_data)

    assert len(df["Event_Id"]) == 1
    assert isinstance(df["Event_Id"].iloc[0], str)
    assert len(df["Event_Id"].iloc[0]) == 32
    assert df["Event_Id"].iloc[0].isalnum()
    assert df["Event_Id"].iloc[0].islower()
