"""
Test the create_nulls_in_measure_validation_event function 
"""
from uuid import UUID
import pandas as pd
import pytest

from dqchecks.panacea import create_nulls_in_measure_validation_event  # Adjust import

valid_metadata = {
    "Batch_Id": "B001",
    "Submission_Period_Cd": "2024Q4",
    "Process_Cd": "pr24bpt",
    "Template_Version": "1.0",
    "Organisation_Cd": "ORG123",
}


def test_returns_none_when_no_nulls():
    """test_returns_none_when_no_nulls"""
    df = pd.DataFrame({
        "Measure_Cd": ["A"],
        "Measure_Desc": ["desc"],
        "Measure_Unit": ["unit"],
        "Sheet_Cd": ["S1"],
        "Cell_Cd": ["C1"]
    })
    result = create_nulls_in_measure_validation_event(df, valid_metadata)
    assert result.empty


def test_returns_dataframe_when_nulls_found():
    """test_returns_dataframe_when_nulls_found"""
    df = pd.DataFrame({
        "Measure_Cd": [None, "B"],
        "Measure_Desc": ["desc", "desc2"],
        "Measure_Unit": ["unit", "unit"],
        "Sheet_Cd": ["S1", "S2"],
        "Cell_Cd": ["C1", "C2"]
    })
    result = create_nulls_in_measure_validation_event(df, valid_metadata)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 1
    assert "Error_Desc" in result.columns
    assert "S1 -- C1" in result["Error_Desc"].iloc[0]
    assert UUID(result["Event_Id"].iloc[0])  # Valid UUID


def test_raises_on_missing_dataframe_columns():
    """test_raises_on_missing_dataframe_columns"""
    df = pd.DataFrame({
        "Measure_Cd": ["A"],
        "Measure_Desc": ["desc"]
    })
    with pytest.raises(ValueError, match="Missing required columns in input DataFrame"):
        create_nulls_in_measure_validation_event(df, valid_metadata)


def test_raises_on_invalid_df_type():
    """test_raises_on_invalid_df_type"""
    with pytest.raises(ValueError, match="must be a pandas DataFrame"):
        create_nulls_in_measure_validation_event("not a dataframe", valid_metadata)


def test_returns_none_on_empty_dataframe():
    """test_returns_none_on_empty_dataframe"""
    df = pd.DataFrame(columns=["Measure_Cd", "Measure_Desc", "Measure_Unit", "Sheet_Cd", "Cell_Cd"])
    result = create_nulls_in_measure_validation_event(df, valid_metadata)
    assert result.empty


def test_fallback_metadata_values():
    """test_fallback_metadata_values"""
    df = pd.DataFrame({
        "Measure_Cd": [None],
        "Measure_Desc": [None],
        "Measure_Unit": ["unit"],
        "Sheet_Cd": ["S1"],
        "Cell_Cd": ["C1"]
    })
    result = create_nulls_in_measure_validation_event(df, {})
    assert result["Batch_Id"].iloc[0] == "--missing--"
