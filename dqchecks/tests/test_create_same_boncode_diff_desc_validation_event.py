"""
Test the create_same_boncode_diff_desc_validation_event function
"""
import pytest
import pandas as pd
from dqchecks.panacea import create_same_boncode_diff_desc_validation_event

def test_raises_on_missing_columns():
    """test_raises_on_missing_columns"""
    df = pd.DataFrame({'Measure_Cd': ['A']})  # Missing Measure_Desc, Sheet_Cd
    metadata = {}
    with pytest.raises(ValueError, match="Missing required columns"):
        create_same_boncode_diff_desc_validation_event(df, metadata)

def test_returns_empty_df_on_empty_input():
    """test_returns_empty_df_on_empty_input"""
    df = pd.DataFrame(columns=['Measure_Cd', 'Measure_Desc', 'Sheet_Cd'])
    metadata = {}
    result = create_same_boncode_diff_desc_validation_event(df, metadata)
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_returns_empty_df_when_no_duplicates_found():
    """test_returns_empty_df_when_no_duplicates_found"""
    df = pd.DataFrame({
        'Measure_Cd': ['A', 'B', 'C'],
        'Measure_Desc': ['desc1', 'desc2', 'desc3'],
        'Sheet_Cd': ['S1', 'S2', 'S3'],
    })
    metadata = {}
    result = create_same_boncode_diff_desc_validation_event(df, metadata)
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_returns_validation_event_when_duplicates_found():
    """test_returns_validation_event_when_duplicates_found"""
    df = pd.DataFrame({
        'Measure_Cd': ['A', 'A', 'B'],
        'Measure_Desc': ['desc1', 'desc2', 'desc3'],
        'Sheet_Cd': ['S1', 'S2', 'S3'],
    })
    metadata = {
        "Batch_Id": "batch1",
        "Submission_Period_Cd": "period1",
        "Process_Cd": "proc1",
        "Template_Version": "v1",
        "Organisation_Cd": "org1"
    }

    result = create_same_boncode_diff_desc_validation_event(df, metadata)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

    # Check that error description contains Measure_Cd and sheets
    error_desc_col = 'Error_Desc'
    assert error_desc_col in result.columns
    assert any("A:[" in str(val) for val in result[error_desc_col].values)

def test_raises_if_input_not_dataframe():
    """test_raises_if_input_not_dataframe"""
    not_a_df = ["not", "a", "dataframe"]
    with pytest.raises(ValueError, match="Input 'df' must be a pandas DataFrame."):
        create_same_boncode_diff_desc_validation_event(not_a_df, {})

def test_raises_if_metadata_not_dict():
    """test_raises_if_metadata_not_dict"""
    df = pd.DataFrame({
        "Measure_Cd": ["B1"],
        "Measure_Desc": ["Desc2"],
        "Sheet_Cd": ["Sheet3    "]
    })
    not_metadata = ["not", "a", "dict"]

    with pytest.raises(ValueError, match="Input 'metadata' must be a dict."):
        create_same_boncode_diff_desc_validation_event(df, not_metadata)
