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
    # pylint: disable=C0301
    """Test that validation event is returned for Measure_Cd used with multiple Measure_Desc values."""
    df = pd.DataFrame({
        'Measure_Cd': ['A', 'A', 'B'],
        'Measure_Desc': ['desc1', 'desc2', 'desc3'],
        'Sheet_Cd': ['S1', 'S2', 'S3'],
        "Cell_Cd": ["A1", "A2", "A3"],
    })

    metadata = {
        "Batch_Id": "batch1",
        "Submission_Period_Cd": "period1",
        "Process_Cd": "proc1",
        "Template_Version": "v1",
        "Organisation_Cd": "org1",
        "Validation_Processing_Stage": "stage1"
    }

    result = create_same_boncode_diff_desc_validation_event(df, metadata)

    # Basic checks
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert 'Error_Desc' in result.columns
    assert 'Event_Id' in result.columns

    # Check number of rows: expect 2 rows (for desc1 and desc2)
    assert len(result) == 2

    # Check that each row corresponds to 'A' with a distinct Measure_Desc
    for desc in ['desc1', 'desc2']:
        assert any(desc in str(val) for val in result['Error_Desc'].values)

    # Check that Measure_Cd 'A' is mentioned in the description
    assert all("Measure_Cd 'A'" in str(val) for val in result['Error_Desc'].values)

    # Check that Event_Id is unique
    assert result['Event_Id'].is_unique


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
