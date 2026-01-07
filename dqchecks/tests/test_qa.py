"""Tests for dqchecks.qa reusable QA logic."""
# pylint: disable=line-too-long


from datetime import datetime
import pandas as pd
import pytest

from dqchecks import qa


def _make_basic_input_frames_with_missing_and_extra():
    """
    Helper to build small flat/semantic DataFrames where:
      - M1 matches on both sides (no diff)
      - M2 has a Measure_Value mismatch
      - M3 exists only in flat (MISSING_IN_INGESTED)
      - M4 exists only in semantic (EXTRA_IN_INGESTED)
    All rows belong to the same submission period '2025Q1'.
    """
    # Flat_File-style combined data
    combined_df = pd.DataFrame(
        [
            # M1 – same value on both sides -> no diff
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M1",
                "Measure_Desc": "Desc 1",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
            # M2 – value mismatch
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M2",
                "Measure_Desc": "Desc 2",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "20%",
                "Sheet_Cd": "Sheet1",
            },
            # M3 – only in flat
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M3",
                "Measure_Desc": "Desc 3",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "30%",
                "Sheet_Cd": "Sheet1",
            },
            # Different submission period – should be filtered out
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2024Q4",
                "Observation_Period_Cd": "202401",
                "Measure_Cd": "M4",
                "Measure_Desc": "Desc 4",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "40%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    # Semantic flattened data
    ingested_df_flat = pd.DataFrame(
        [
            # M1 – matches flat
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M1",
                "Measure_Name": "Desc 1",
                "Unit": "%",
                "Decimal_Point": 2,
                # Store measure as "10" so 10/100 = 0.1 (10%)
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
            },
            # M2 – different measure value ("30" -> 30%)
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M2",
                "Measure_Name": "Desc 2",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "30",
                "Sheet_Cd": "Sheet1",
            },
            # M4 – only in semantic
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M4",
                "Measure_Name": "Desc 4",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "40",
                "Sheet_Cd": "Sheet1",
            },
            # Different submission period – should be filtered out
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2024Q4",
                "Observation_Period_Cd": "202401",
                "Legacy_Measure_Reference": "M5",
                "Measure_Name": "Desc 5",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "50",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    return combined_df, ingested_df_flat


def _make_basic_input_frames_for_summary():
    """
    Helper to build flat/semantic DataFrames where:
      - Two keys exist on both sides (M1, M2)
      - Only M2 has a Measure_Value mismatch
      - No missing / extra keys
    This is used to exercise build_qa_summaries in a clean way.
    """
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M1",
                "Measure_Desc": "Desc 1",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M2",
                "Measure_Desc": "Desc 2",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "20%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    ingested_df_flat = pd.DataFrame(
        [
            # M1 – matches value
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M1",
                "Measure_Name": "Desc 1",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
            },
            # M2 – different value
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M2",
                "Measure_Name": "Desc 2",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "30",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    return combined_df, ingested_df_flat


def test_prepare_qa_frames_filters_and_builds_measure_key():
    """prepare_qa_frames should filter by submission period and add Measure_Key."""
    combined_df, ingested_df_flat = _make_basic_input_frames_with_missing_and_extra()

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )

    # Only rows for 2025Q1 are kept
    assert flat_for_qa["Submission_Period_Cd"].unique().tolist() == ["2025Q1"]
    assert sem_for_qa["Submission_Period_Cd"].unique().tolist() == ["2025Q1"]

    # Measure_Key exists and is derived from Measure_Cd / Legacy_Measure_Reference
    assert "Measure_Key" in flat_for_qa.columns
    assert "Measure_Key" in sem_for_qa.columns

    # All Region_Cd blanks should have been normalised to 'NA'
    assert set(flat_for_qa["Region_Cd"].unique()) == {"NA"}
    assert set(sem_for_qa["Region_Cd"].unique()) == {"NA"}


def test_compute_key_overlap_identifies_only_raw_sem_and_both():
    """compute_key_overlap should correctly identify keys only in raw, only in semantic, and in both."""
    combined_df, ingested_df_flat = _make_basic_input_frames_with_missing_and_extra()

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )

    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    # After normalisation, we expect:
    #   - keys in both: M1, M2
    #   - only in raw: M3
    #   - only in sem: M4
    both_keys = set(keys_in_both["Measure_Key"].tolist())
    only_raw_keys = set(keys_only_raw["Measure_Key"].tolist())
    only_sem_keys = set(keys_only_sem["Measure_Key"].tolist())

    assert both_keys == {"M1", "M2"}
    assert only_raw_keys == {"M3"}
    assert only_sem_keys == {"M4"}


def test_build_qa_diff_missing_extra_and_value_mismatch():
    """build_qa_diff should emit rows for missing, extra, and measure value mismatches."""
    combined_df, ingested_df_flat = _make_basic_input_frames_with_missing_and_extra()

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )

    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH1",
        qa_run_datetime=datetime(2025, 1, 1).isoformat(),
    )

    # We expect:
    #   - 1 MISSING_IN_INGESTED (M3)
    #   - 1 EXTRA_IN_INGESTED (M4)
    #   - 1 MEASURE_VALUE_MISMATCH (M2)
    assert not qa_diff_df.empty
    assert set(qa_diff_df["Error_Type"].unique()) == {
        "MISSING_IN_INGESTED",
        "EXTRA_IN_INGESTED",
        "MEASURE_VALUE_MISMATCH",
    }

    # Check that Batch_Id and QA_Run_Datetime were stamped
    assert (qa_diff_df["Batch_Id"] == "BATCH1").all()
    assert "QA_Run_Datetime" in qa_diff_df.columns

    # Sanity-check one of each error type
    missing_row = qa_diff_df[qa_diff_df["Error_Type"] == "MISSING_IN_INGESTED"].iloc[0]
    assert missing_row["Measure_Key"] == "M3"
    assert missing_row["Raw_Value"] == "30%"
    assert pd.isna(missing_row["Ingested_Value"])

    extra_row = qa_diff_df[qa_diff_df["Error_Type"] == "EXTRA_IN_INGESTED"].iloc[0]
    assert extra_row["Measure_Key"] == "M4"
    assert extra_row["Raw_Value"] is None
    assert extra_row["Ingested_Value"] == "40"

    mismatch_row = qa_diff_df[qa_diff_df["Error_Type"] == "MEASURE_VALUE_MISMATCH"].iloc[0]
    assert mismatch_row["Measure_Key"] == "M2"
    assert mismatch_row["Raw_Value"] == "20%"
    assert mismatch_row["Ingested_Value"] == "30"


def test_build_qa_diff_missing_company_from_folder():
    """build_qa_diff should create synthetic rows for missing companies from folder."""
    # No actual data needed for this path – the logic only depends on file names and expected companies.
    flat_for_qa = pd.DataFrame(columns=qa.KEY_COLS)
    sem_for_qa = pd.DataFrame(columns=qa.KEY_COLS)
    keys_only_raw = flat_for_qa.copy()
    keys_only_sem = sem_for_qa.copy()
    keys_in_both = flat_for_qa.copy()

    filtered_excel_files = [
        "/some/path/ORG1 Quarterly data template 2025-26 Q2 31-10-2025 v1.xlsx",
    ]
    expected_companies = ["ORG1", "ORG2"]

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_MISSING",
        qa_run_datetime="2025-01-01T00:00:00",
        filtered_excel_files=filtered_excel_files,
        expected_companies=expected_companies,
        status="complete",
        process_cd="apr",
        submission_period_cd="2025-26 Q2",
        target_submission_period="2025-26 Q2",
    )

    # ORG2 is expected but not present in any filename -> one synthetic error row
    assert len(qa_diff_df) == 1
    row = qa_diff_df.iloc[0]
    assert row["Organisation_Cd"] == "ORG2"
    assert row["Error_Type"] == "MISSING_COMPANY_FROM_FOLDER"
    assert row["Column_Name"] == "Organisation_Cd"
    assert "Missing Company From Folder" in row["Error_Desc"]


def test_build_qa_summaries_basic_counts_and_breakdown():
    """
    build_qa_summaries should compute overall counts, per-company summary,
    and error counts broken down by company and error type.
    """
    combined_df, ingested_df_flat = _make_basic_input_frames_for_summary()

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )

    _, _, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=pd.DataFrame(columns=qa.KEY_COLS),
        keys_only_sem=pd.DataFrame(columns=qa.KEY_COLS),
        keys_in_both=keys_in_both,
        batch_id="BATCH_SUMMARY",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    qa_summary_df, qa_company_summary_df, error_counts_df = qa.build_qa_summaries(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_in_both=keys_in_both,
        qa_diff_df=qa_diff_df,
        batch_id="BATCH_SUMMARY",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    # --- Global summary ---
    assert len(qa_summary_df) == 1
    summary = qa_summary_df.iloc[0]

    # There are 2 rows on each side, 2 keys in both, 1 mismatched row, 1 matched row
    assert summary["Total_Raw_Rows"] == 2
    assert summary["Total_Ingested_Rows"] == 2
    assert summary["Rows_With_Keys_In_Both"] == 2
    assert summary["Total_Rows_With_Mismatches"] == 1
    assert summary["Total_Matched_Rows"] == 1
    assert summary["Total_Cell_Level_Differences"] == 1
    assert summary["Columns_Affected"] == "Measure_Value"
    assert summary["Error_Types"] == "MEASURE_VALUE_MISMATCH"

    # --- Per-company summary ---
    assert len(qa_company_summary_df) == 1
    comp = qa_company_summary_df.iloc[0]
    assert comp["Organisation_Cd"] == "ORG1"
    assert comp["Total_Raw_Rows"] == 2
    assert comp["Total_Ingested_Rows"] == 2
    assert comp["Rows_With_Keys_In_Both"] == 2
    assert comp["Total_Rows_With_Mismatches"] == 1
    assert comp["Total_Matched_Rows"] == 1
    assert comp["Total_Cell_Level_Differences"] == 1
    assert comp["Columns_Affected"] == "Measure_Value"
    assert comp["Error_Types"] == "MEASURE_VALUE_MISMATCH"

    # --- Error counts breakdown ---
    assert len(error_counts_df) == 1
    err_row = error_counts_df.iloc[0]
    assert err_row["Organisation_Cd"] == "ORG1"
    assert err_row["Error_Type"] == "MEASURE_VALUE_MISMATCH"
    assert err_row["Error_Count"] == 1
    assert err_row["Batch_Id"] == "BATCH_SUMMARY"


def test_prepare_qa_frames_filters_by_org_and_normalises_period_codes():
    """prepare_qa_frames should filter by org and normalise period codes with '.0'."""
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "  ",
                "Submission_Period_Cd": "2025Q1.0",
                "Observation_Period_Cd": "202501.0",
                "Measure_Cd": "M1",
                "Measure_Desc": "Desc 1",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
            {
                "Organisation_Cd": "ORG2",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1.0",
                "Observation_Period_Cd": "202501.0",
                "Measure_Cd": "M2",
                "Measure_Desc": "Desc 2",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "20%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "   ",
                "Submission_Period_Cd": "2025Q1.0",
                "Observation_Period_Cd": "202501.0",
                "Legacy_Measure_Reference": "M1",
                "Measure_Name": "Desc 1",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
                "Insert_Date": "2025-01-01",
            },
            {
                "Organisation_Cd": "ORG2",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1.0",
                "Observation_Period_Cd": "202501.0",
                "Legacy_Measure_Reference": "M2",
                "Measure_Name": "Desc 2",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "20",
                "Sheet_Cd": "Sheet1",
                "Insert_Date": "2025-01-02",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
        target_org="ORG1",
    )

    # Only ORG1 kept
    assert flat_for_qa["Organisation_Cd"].unique().tolist() == ["ORG1"]
    assert sem_for_qa["Organisation_Cd"].unique().tolist() == ["ORG1"]

    # Period codes should have '.0' stripped
    assert flat_for_qa["Submission_Period_Cd"].unique().tolist() == ["2025Q1"]
    assert flat_for_qa["Observation_Period_Cd"].unique().tolist() == ["202501"]
    assert sem_for_qa["Submission_Period_Cd"].unique().tolist() == ["2025Q1"]
    assert sem_for_qa["Observation_Period_Cd"].unique().tolist() == ["202501"]

    # Region blanks -> "NA"
    assert set(flat_for_qa["Region_Cd"].unique()) == {"NA"}
    assert set(sem_for_qa["Region_Cd"].unique()) == {"NA"}


def test_build_qa_diff_measure_decimals_mismatch_only():
    """
    build_qa_diff should detect MEASURE_DECIMALS_MISMATCH when decimals differ
    but values normalised to same numeric.
    """
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M_DEC",
                "Measure_Desc": "Decimals test",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M_DEC",
                "Measure_Name": "Decimals test",
                "Unit": "%",
                "Decimal_Point": 3,  # different decimals
                "Measure_Value": "10.0",  # same logical value
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_DEC",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    # We expect only a decimals mismatch, no measure value mismatch
    assert set(qa_diff_df["Error_Type"].unique()) == {"MEASURE_DECIMALS_MISMATCH"}


def test_build_qa_diff_description_unit_and_comment_mismatch():
    """
    build_qa_diff should hit DESCRIPTION_MISMATCH, UNIT_DATATYPE_MISMATCH,
    and generic COLUMN_MISMATCH for Comment.
    """
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M_STR",
                "Measure_Desc": "Original desc",
                "Measure_Unit": "m3",
                "Measure_Decimals": 2,
                "Measure_Value": "100",
                "Comment": "Raw comment",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M_STR",
                "Measure_Name": "Different desc",  # -> DESCRIPTION_MISMATCH
                "Unit": "m3/s",                    # -> UNIT_DATATYPE_MISMATCH
                "Decimal_Point": 2,
                "Measure_Value": "100",
                "Measure_Comment": "Semantic comment",  # -> COMMENT_MISMATCH
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_STR",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    error_types = set(qa_diff_df["Error_Type"].unique())
    assert "DESCRIPTION_MISMATCH" in error_types
    assert "UNIT_DATATYPE_MISMATCH" in error_types
    # Comment becomes generic COMMENT_MISMATCH
    assert "COMMENT_MISMATCH" in error_types


def test_build_qa_diff_no_differences_returns_empty():
    """If there are no missing/extra rows and no column diffs, build_qa_diff should return empty df."""
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M1",
                "Measure_Desc": "Desc 1",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M1",
                "Measure_Name": "Desc 1",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_NODIFF",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    assert qa_diff_df.empty


def test_build_qa_summaries_when_no_differences():
    """build_qa_summaries with empty qa_diff_df should report zero mismatches and empty error tables."""
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M1",
                "Measure_Desc": "Desc 1",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M1",
                "Measure_Name": "Desc 1",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    _, _, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    empty_diff = pd.DataFrame()

    qa_summary_df, qa_company_summary_df, error_counts_df = qa.build_qa_summaries(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_in_both=keys_in_both,
        qa_diff_df=empty_diff,
        batch_id="BATCH_EMPTY",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    summary = qa_summary_df.iloc[0]
    assert summary["Total_Raw_Rows"] == 1
    assert summary["Total_Ingested_Rows"] == 1
    assert summary["Rows_With_Keys_In_Both"] == 1
    assert summary["Total_Rows_With_Mismatches"] == 0
    assert summary["Total_Matched_Rows"] == 1
    assert summary["Total_Cell_Level_Differences"] == 0
    assert summary["Columns_Affected"] == ""
    assert summary["Error_Types"] == ""

    # Per-company summary still has counts but zero mismatches
    comp = qa_company_summary_df.iloc[0]
    assert comp["Organisation_Cd"] == "ORG1"
    assert comp["Total_Raw_Rows"] == 1
    assert comp["Total_Ingested_Rows"] == 1
    assert comp["Rows_With_Keys_In_Both"] == 1
    assert comp["Total_Rows_With_Mismatches"] == 0
    assert comp["Total_Matched_Rows"] == 1
    assert comp["Total_Cell_Level_Differences"] == 0

    # No error counts rows
    assert error_counts_df.empty


def test_build_qa_summaries_multiple_organisations():
    """build_qa_summaries should break down counts correctly across multiple organisations."""
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M1",
                "Measure_Desc": "Desc 1",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
            {
                "Organisation_Cd": "ORG2",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M2",
                "Measure_Desc": "Desc 2",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "20%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            # ORG1 – matching row
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M1",
                "Measure_Name": "Desc 1",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
            },
            # ORG2 – mismatching value
            {
                "Organisation_Cd": "ORG2",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M2",
                "Measure_Name": "Desc 2",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "30",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    _, _, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=pd.DataFrame(columns=qa.KEY_COLS),
        keys_only_sem=pd.DataFrame(columns=qa.KEY_COLS),
        keys_in_both=keys_in_both,
        batch_id="BATCH_MULTI",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    _, qa_company_summary_df, error_counts_df = qa.build_qa_summaries(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_in_both=keys_in_both,
        qa_diff_df=qa_diff_df,
        batch_id="BATCH_MULTI",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    # ORG1 should have no mismatches, ORG2 should have 1
    org1_row = qa_company_summary_df[qa_company_summary_df["Organisation_Cd"] == "ORG1"].iloc[0]
    org2_row = qa_company_summary_df[qa_company_summary_df["Organisation_Cd"] == "ORG2"].iloc[0]

    assert org1_row["Total_Rows_With_Mismatches"] == 0
    assert org1_row["Total_Matched_Rows"] == 1

    assert org2_row["Total_Rows_With_Mismatches"] == 1
    assert org2_row["Total_Matched_Rows"] == 0

    # Error counts should only show ORG2
    assert set(error_counts_df["Organisation_Cd"].unique()) == {"ORG2"}
    assert error_counts_df["Error_Type"].unique().tolist() == ["MEASURE_VALUE_MISMATCH"]
    assert error_counts_df["Error_Count"].iloc[0] == 1


def test_build_qa_diff_handles_missing_decimals_columns():
    """
    build_qa_diff should still work if Measure_Decimals columns are missing.
    This exercises the fallback branches where Measure_Decimals_* are absent.
    """
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M_NO_DEC",
                "Measure_Desc": "No decimals col",
                "Measure_Unit": "%",      # keep unit so % normalisation still happens
                # NOTE: deliberately no Measure_Decimals column
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M_NO_DEC",
                "Measure_Name": "No decimals col",
                "Unit": "%",              # maps to Measure_Unit
                # NOTE: deliberately no Decimal_Point column
                "Measure_Value": "10",    # logically same as 10%
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )

    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    # We have one overlapping key
    assert not keys_in_both.empty

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_NO_DEC",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    # Values are logically equivalent -> no diffs, but the missing-decimals branches are executed
    assert qa_diff_df.empty


def test_build_qa_diff_uses_ingested_measure_desc_when_raw_missing():
    """
    When Measure_Desc_raw is missing but Measure_Desc_ingested is present,
    build_qa_diff should use the ingested description (exercise the `else` branch).
    """
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M_NO_DESC",
                "Measure_Desc": None,         # raw desc missing
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",       # <- raw value
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M_NO_DESC",
                "Measure_Name": "Ingested desc",  # <- this should be picked
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "20",            # different -> force a diff
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_DESC_FALLBACK",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    assert not qa_diff_df.empty

    # We expect both a description mismatch and a measure value mismatch
    error_types = set(qa_diff_df["Error_Type"].unique())
    assert "MEASURE_VALUE_MISMATCH" in error_types
    assert "DESCRIPTION_MISMATCH" in error_types

    # Specifically check the MEASURE_VALUE_MISMATCH row uses the ingested description
    mv_row = qa_diff_df[qa_diff_df["Error_Type"] == "MEASURE_VALUE_MISMATCH"].iloc[0]
    assert mv_row["Measure_Desc"] == "Ingested desc"


def test_build_qa_summaries_when_no_keys_in_both_any_org():
    """
    build_qa_summaries should handle the case where there are no shared keys
    (keys_in_both empty), exercising the fallback `keys_in_both_by_org` branch.
    """
    # ORG1 only in flat, ORG2 only in semantic -> no overlap
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M_RAW_ONLY",
                "Measure_Desc": "Raw only",
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG2",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M_SEM_ONLY",
                "Measure_Name": "Sem only",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "20",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )

    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    assert keys_in_both.empty  # sanity

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_NO_KEYS_IN_BOTH",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    qa_summary_df, qa_company_summary_df, error_counts_df = qa.build_qa_summaries(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_in_both=keys_in_both,
        qa_diff_df=qa_diff_df,
        batch_id="BATCH_NO_KEYS_IN_BOTH",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    # Global summary: zero rows with keys in both
    assert qa_summary_df.iloc[0]["Rows_With_Keys_In_Both"] == 0

    # Per-company summary: both orgs appear, and the 'Rows_With_Keys_In_Both' col exists
    assert set(qa_company_summary_df["Organisation_Cd"].unique()) == {"ORG1", "ORG2"}
    assert "Rows_With_Keys_In_Both" in qa_company_summary_df.columns
    assert (qa_company_summary_df["Rows_With_Keys_In_Both"] == 0).all()

    # Error counts table should be non-empty and not crash
    assert not error_counts_df.empty


# pylint: disable=protected-access
def test_normalise_keys_with_measure_raises_if_measure_col_missing():
    """_normalise_keys_with_measure should error if the measure column is absent."""
    df = pd.DataFrame({"Organisation_Cd": ["ORG1"]})
    with pytest.raises(ValueError, match="Measure_Cd not found when building Measure_Key"):
        qa._normalise_keys_with_measure(df, measure_col="Measure_Cd")


def test_normalise_measure_value_without_unit_series():
    """_normalise_measure_value should work when unit_series is None."""
    series = pd.Series(["10%", "2.5", "bad", None])
    result = qa._normalise_measure_value(series)

    # No unit series -> '%' is *stripped* but not scaled by 1/100.
    assert result.iloc[0] == 10.0
    assert result.iloc[1] == 2.5
    assert pd.isna(result.iloc[2])
    assert pd.isna(result.iloc[3])


def test_build_qa_diff_uses_fallback_measure_desc_on_exception(monkeypatch):
    """Force the Measure_Desc selection to go through the defensive except branch."""
    combined_df = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Measure_Cd": "M_DESC",
                "Measure_Desc": "BOOM",  # this will trigger our patched pd.notna
                "Measure_Unit": "%",
                "Measure_Decimals": 2,
                "Measure_Value": "10%",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )
    ingested_df_flat = pd.DataFrame(
        [
            {
                "Organisation_Cd": "ORG1",
                "Region_Cd": "",
                "Submission_Period_Cd": "2025Q1",
                "Observation_Period_Cd": "202501",
                "Legacy_Measure_Reference": "M_DESC",
                "Measure_Name": "Different desc",
                "Unit": "%",
                "Decimal_Point": 2,
                "Measure_Value": "10",
                "Sheet_Cd": "Sheet1",
            },
        ]
    )

    # Capture original notna before patch
    original_notna = pd.notna

    # Patch qa.pd.notna so it explodes on "BOOM" and triggers the except branch.
    def bad_notna(value):
        if value == "BOOM":
            raise RuntimeError("boom")
        return original_notna(value)

    monkeypatch.setattr(qa.pd, "notna", bad_notna)

    # --- Hit the non-error branch at least once so coverage sees `return original_notna` ---
    assert qa.pd.notna("SAFE") is True

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(
        combined_df=combined_df,
        ingested_df_flat=ingested_df_flat,
        target_submission_period="2025Q1",
    )
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
    )

    qa_diff_df = qa.build_qa_diff(
        flat_for_qa=flat_for_qa,
        sem_for_qa=sem_for_qa,
        keys_only_raw=keys_only_raw,
        keys_only_sem=keys_only_sem,
        keys_in_both=keys_in_both,
        batch_id="BATCH_DESC",
        qa_run_datetime="2025-01-01T00:00:00",
    )

    # If the fallback branch ran, Measure_Desc should still be "BOOM" rather than exploding.
    assert "BOOM" in qa_diff_df["Measure_Desc"].unique()
