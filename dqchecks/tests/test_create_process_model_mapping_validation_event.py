"""
Test the create_process_model_mapping_validation_event function
"""
import pytest
import pandas as pd
from dqchecks.panacea import create_process_model_mapping_validation_event


@pytest.fixture
def valid_metadata():
    """valid_metadata"""
    return {
        "Batch_Id": "B123",
        "Submission_Period_Cd": "202401",
        "Process_Cd": "PROC1",
        "Template_Version": "v1.0",
        "Organisation_Cd": "ORGX",
        "Validation_Processing_Stage": "Validation_Processing_Stage",
    }


# pylint: disable=W0621
def test_raises_if_df_not_dataframe(valid_metadata):
    """test_raises_if_df_not_dataframe"""
    with pytest.raises(ValueError, match="Input 'df' must be a pandas DataFrame."):
        create_process_model_mapping_validation_event("not_a_df", {}, valid_metadata)


def test_raises_if_metadata_not_dict():
    """test_raises_if_metadata_not_dict"""
    df = pd.DataFrame(columns=["Process_Cd", "Model_Cd"])
    with pytest.raises(ValueError, match="Input 'metadata' must be a dict."):
        create_process_model_mapping_validation_event(df, {}, "not_a_dict")


# pylint: disable=W0621
def test_raises_if_mapping_not_dict(valid_metadata):
    """test_raises_if_mapping_not_dict"""
    df = pd.DataFrame(columns=["Process_Cd", "Model_Cd"])
    with pytest.raises(ValueError, match="Input 'process_model_mapping' must be a dict."):
        create_process_model_mapping_validation_event(df, "not_a_dict", valid_metadata)


# pylint: disable=W0621
def test_raises_if_required_columns_missing(valid_metadata):
    """test_raises_if_required_columns_missing"""
    df = pd.DataFrame(columns=["Process_Cd"])
    with pytest.raises(ValueError, match="Missing required columns in input DataFrame"):
        create_process_model_mapping_validation_event(df, {}, valid_metadata)


# pylint: disable=W0621
def test_returns_empty_df_on_empty_input(valid_metadata):
    """test_returns_empty_df_on_empty_input"""
    df = pd.DataFrame(columns=["Process_Cd", "Model_Cd"])
    result = create_process_model_mapping_validation_event(df, {}, valid_metadata)
    assert isinstance(result, pd.DataFrame)
    assert result.empty


# pylint: disable=W0621
def test_returns_event_if_multiple_mappings_found(valid_metadata):
    """test_returns_event_if_multiple_mappings_found"""
    df = pd.DataFrame({
        "Process_Cd": ["PROC1", "PROC1"],
        "Model_Cd": ["MODEL1", "MODEL2"]
    })
    result = create_process_model_mapping_validation_event(df, {"PROC1": "MODEL1"}, valid_metadata)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "Expected exactly 1 unique mapping" in result.iloc[0]["Error_Desc"]


# pylint: disable=W0621
def test_returns_event_if_mapping_mismatch(valid_metadata):
    """test_returns_event_if_mapping_mismatch"""
    df = pd.DataFrame({
        "Process_Cd": ["PROC1"],
        "Model_Cd": ["WRONG_MODEL"]
    })
    result = create_process_model_mapping_validation_event(df, {"PROC1": "MODEL1"}, valid_metadata)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "Expected 'MODEL1'" in result.iloc[0]["Error_Desc"]


# pylint: disable=W0621
def test_returns_empty_df_if_mapping_valid(valid_metadata):
    """test_returns_empty_df_if_mapping_valid"""
    df = pd.DataFrame({
        "Process_Cd": ["PROC1"],
        "Model_Cd": ["MODEL1"]
    })
    result = create_process_model_mapping_validation_event(df, {"PROC1": "MODEL1"}, valid_metadata)
    assert isinstance(result, pd.DataFrame)
    assert result.empty
