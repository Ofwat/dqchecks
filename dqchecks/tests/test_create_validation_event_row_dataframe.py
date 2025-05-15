"""tests of the create_validation_event_row_dataframe function"""
import pytest
import pandas as pd
from dqchecks.utils import create_validation_event_row_dataframe

def test_all_none_when_no_input():
    """test_all_none_when_no_input"""
    df = create_validation_event_row_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 19)
    assert df.isnull().all().all()

def test_partial_input():
    """test_partial_input"""
    df = create_validation_event_row_dataframe(Event_Id=1, Error_Desc="Missing value")
    assert df.loc[0, "Event_Id"] == 1
    assert df.loc[0, "Error_Desc"] == "Missing value"
    # Ensure others are None
    assert df.loc[0, "Batch_Id"] is None
    assert df.shape == (1, 19)

def test_full_input():
    """test_full_input"""
    input_data = {
        "Event_Id": 101,
        "Batch_Id": "B-001",
        "Validation_Processing_Stage": "Stage 1",
        "Sheet_Cd": "SH1",
        "Template_Version": "v1.0",
        "Rule_Cd": "R001",
        "Organisation_Cd": "ORG123",
        "Measure_Cd": "M001",
        "Measure_Unit": "Units",
        "Measure_Desc": "Sample Measure",
        "Submission_Period_Cd": "202401",
        "Process_Cd": "P001",
        "Error_Category": "Critical",
        "Section_Cd": "SEC01",
        "Cell_Cd": "C01",
        "Data_Column": "Value",
        "Error_Value": "XYZ",
        "Error_Severity_Cd": "High",
        "Error_Desc": "Invalid data"
    }
    df = create_validation_event_row_dataframe(**input_data)
    for key, val in input_data.items():
        assert df.loc[0, key] == val
    assert df.shape == (1, 19)

def test_invalid_column_name_raises_error():
    """test_invalid_column_name_raises_error"""
    with pytest.raises(ValueError) as excinfo:
        create_validation_event_row_dataframe(Invalid_Column="test")
    assert "Invalid column names provided" in str(excinfo.value)
