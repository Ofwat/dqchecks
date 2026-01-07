"""
dqchecks.qa

Reusable QA logic for comparing Flat_File (Excel-derived) data
against semantic / ingested data.

Designed to be called from Fabric notebooks, e.g.:

    from dqchecks import qa

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(...)
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(...)
    qa_diff_df = qa.build_qa_diff(...)
    (
        qa_summary_df,
        qa_company_summary_df,
        error_counts_df,
    ) = qa.build_qa_summaries(...)

This module is intentionally pure-pandas (no Fabric / Spark / DB engine).
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd

# We intentionally expose orchestration-style functions that take several arguments
# and have branching logic. Suppress corresponding structural warnings.
# Also relax line length and superfluous-parens for readability in f-strings.
# pylint: disable=too-many-arguments,too-many-positional-arguments
# pylint: disable=too-many-locals,too-many-branches,too-many-statements
# pylint: disable=line-too-long,superfluous-parens

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# COLUMN CONSTANTS
# --------------------------------------------------------------------------------------

# Columns we care about from Flat_File / semantic
COMPARE_COLS: list[str] = [
    "Organisation_Cd",
    "Region_Cd",
    "Template_Version",
    "Sheet_Cd",
    "Submission_Date",
    "Assured",
    "Security_Mark",
    "Process_Cd",
    "Data_Source",
    "Measure_Name",
    "Measure_Desc",
    "Measure_Area",
    "Observation_Coverage_Desc",
    "Observation_Desc",
    "Year_Type",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Measure_Unit",
    "Measure_Decimals",
    "Cell_Cd",
    "Measure_Value",
    "Comment",
]

# Composite key to define "same row"
# NOTE: we join using Measure_Key, which is:
#   - Flat_File: Measure_Cd
#   - Semantic:  Legacy_Measure_Reference
KEY_COLS: list[str] = [
    "Organisation_Cd",
    "Region_Cd",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Measure_Key",   # <- canonical join key
]

# Context columns shown in the diff output (only if present)
CONTEXT_COLS: list[str] = [
    "Organisation_Cd",
    "Region_Cd",
    "Sheet_Cd",
    "Measure_Cd",                # original Excel measure code
    "Legacy_Measure_Reference",  # semantic legacy code
    "Measure_Key",               # join key
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Cell_Cd",
]

# Mapping from semantic model column names -> Flat_File names
# Important mapping:
#   - Flat_File Measure_Desc  <->  Semantic Measure_Name
SEMANTIC_TO_FLAT_COL_MAP: dict[str, str] = {
    "Data_Source_Desc": "Data_Source",
    "Measure_Name": "Measure_Desc",       # semantic Measure_Name -> Measure_Desc
    "Unit": "Measure_Unit",
    "Decimal_Point": "Measure_Decimals",
    "Measure_Comment": "Comment",
}


# --------------------------------------------------------------------------------------
# HELPER FUNCTIONS (NORMALISATION)
# --------------------------------------------------------------------------------------

def _normalise_period_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise Submission_Period_Cd and Observation_Period_Cd in-place:
      - cast to str
      - strip spaces
      - drop trailing '.0'
    """
    df = df.copy()
    for col in ("Submission_Period_Cd", "Observation_Period_Cd"):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )
    return df


def _normalise_keys_with_measure(df: pd.DataFrame, measure_col: str) -> pd.DataFrame:
    """
    Normalise key columns and build Measure_Key from the specified measure_col.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame (Flat_File or semantic).
    measure_col : str
        - 'Measure_Cd' for Flat_File
        - 'Legacy_Measure_Reference' for Semantic

    Returns
    -------
    DataFrame
        Copy of df with normalised key columns and a 'Measure_Key' column.
    """
    df = df.copy()

    # Organisation
    if "Organisation_Cd" in df.columns:
        df["Organisation_Cd"] = df["Organisation_Cd"].astype(str).str.strip()

    # Region: treat blank as 'NA' to match semantic model
    if "Region_Cd" in df.columns:
        df["Region_Cd"] = df["Region_Cd"].astype(str).str.strip()
        df["Region_Cd"] = df["Region_Cd"].replace("", "NA")

    # Period columns
    for col in ["Submission_Period_Cd", "Observation_Period_Cd"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r"\.0$", "", regex=True)

    # Build Measure_Key from the chosen measure column
    if measure_col not in df.columns:
        raise ValueError(f"{measure_col} not found when building Measure_Key.")

    df["Measure_Key"] = df[measure_col].astype(str).str.strip()

    return df


def _normalise_measure_value(
    value_series: pd.Series,
    unit_series: Optional[pd.Series] = None,
) -> pd.Series:
    """
    Convert measure values to a numeric representation consistent across raw & ingested:

    - Strips '%' symbols.
    - If unit == '%', divide by 100 so '12.5%' -> 0.125.
    - Otherwise, parse numeric as-is.
    """
    s = value_series.astype(str).str.strip()
    s_clean = s.str.replace("%", "", regex=False)
    numeric = pd.to_numeric(s_clean, errors="coerce")

    if unit_series is None:
        return numeric

    u = unit_series.astype(str).str.strip()
    is_pct = (u == "%")

    numeric_pct = numeric / 100.0
    numeric = numeric.where(~is_pct, numeric_pct)
    return numeric


def _normalise_string(s: pd.Series) -> pd.Series:
    """
    Normalise strings for comparison:
      - cast to str
      - fill NaN with ''
      - strip spaces
      - lowercase
    """
    return s.astype(str).fillna("").str.strip().str.lower()


# --------------------------------------------------------------------------------------
# 1) PREPARE DATAFRAMES FOR QA
# --------------------------------------------------------------------------------------

def prepare_qa_frames(
    combined_df: pd.DataFrame,
    ingested_df_flat: pd.DataFrame,
    target_submission_period: str,
    target_org: Optional[str] = None,
    semantic_to_flat_map: Optional[dict[str, str]] = None,
    logger_: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare Flat_File and semantic DataFrames for QA:

    - Renames semantic columns to align with Flat_File.
    - Normalises period codes.
    - Filters by Submission_Period_Cd (and optionally Organisation_Cd).
    - Builds Measure_Key:
          Flat_File: Measure_Cd
          Semantic:  Legacy_Measure_Reference
    - Dedupe semantic by latest Insert_Date per KEY_COLS (if Insert_Date exists).

    Parameters
    ----------
    combined_df : DataFrame
        Flat_File-style data (combined Excel).
    ingested_df_flat : DataFrame
        Flattened semantic model data.
    target_submission_period : str
        Submission period to QA, e.g. "2025-26 Q2".
    target_org : str, optional
        If provided, restrict QA to this Organisation_Cd.
    semantic_to_flat_map : dict, optional
        Mapping from semantic columns to Flat_File columns.
        Defaults to SEMANTIC_TO_FLAT_COL_MAP.
    logger_ : logging.Logger, optional
        Logger to use. Defaults to module logger.

    Returns
    -------
    (flat_for_qa, sem_for_qa) : Tuple[DataFrame, DataFrame]
    """
    log = logger_ or logger

    col_map = semantic_to_flat_map or SEMANTIC_TO_FLAT_COL_MAP

    # 1) Rename semantic columns so they line up with Flat_File column names
    sem_for_qa = ingested_df_flat.rename(columns=col_map).copy()
    flat_for_qa = combined_df.copy()

    # 2) Normalise period codes BEFORE filtering
    flat_for_qa = _normalise_period_codes(flat_for_qa)
    sem_for_qa = _normalise_period_codes(sem_for_qa)

    # 3) Filter by submission period (and optionally company)
    flat_for_qa = flat_for_qa[flat_for_qa["Submission_Period_Cd"] == target_submission_period]
    sem_for_qa = sem_for_qa[sem_for_qa["Submission_Period_Cd"] == target_submission_period]

    if target_org is not None:
        flat_for_qa = flat_for_qa[flat_for_qa["Organisation_Cd"] == target_org]
        sem_for_qa = sem_for_qa[sem_for_qa["Organisation_Cd"] == target_org]

    log.info("Flat_File rows BEFORE key normalisation: %d", len(flat_for_qa))
    log.info("Semantic rows BEFORE key normalisation: %d", len(sem_for_qa))

    # 4) Build Measure_Key (and normalise org/region/period)
    flat_for_qa = _normalise_keys_with_measure(flat_for_qa, measure_col="Measure_Cd")
    sem_for_qa = _normalise_keys_with_measure(sem_for_qa, measure_col="Legacy_Measure_Reference")

    log.info("Flat_File rows AFTER key normalisation: %d", len(flat_for_qa))
    log.info("Semantic rows AFTER key normalisation: %d", len(sem_for_qa))

    # 5) Dedupe semantic by latest Insert_Date
    if "Insert_Date" in sem_for_qa.columns:
        sem_for_qa["_Insert_Date_ts"] = pd.to_datetime(sem_for_qa["Insert_Date"], errors="coerce")
        sem_for_qa = sem_for_qa.sort_values(
            KEY_COLS + ["_Insert_Date_ts"],
            ascending=[True] * len(KEY_COLS) + [False],
        )
        sem_for_qa = sem_for_qa.drop_duplicates(subset=KEY_COLS, keep="first")
        sem_for_qa = sem_for_qa.drop(columns=["_Insert_Date_ts"])

    log.info("Semantic rows AFTER dedupe: %d", len(sem_for_qa))

    return flat_for_qa, sem_for_qa


# --------------------------------------------------------------------------------------
# 2) KEY-LEVEL MATCHING
# --------------------------------------------------------------------------------------

def compute_key_overlap(
    flat_for_qa: pd.DataFrame,
    sem_for_qa: pd.DataFrame,
    logger_: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compute key-level overlap between Flat_File and semantic data.

    Returns three DataFrames containing KEY_COLS only:

    - keys_only_raw : keys only in Flat_File
    - keys_only_sem : keys only in semantic data
    - keys_in_both  : keys present on both sides
    """
    log = logger_ or logger

    raw_keys = flat_for_qa[KEY_COLS].drop_duplicates().copy()
    raw_keys["_side"] = "Flat_File"

    sem_keys = sem_for_qa[KEY_COLS].drop_duplicates().copy()
    sem_keys["_side"] = "Semantic"

    keys_merged = raw_keys.merge(sem_keys, on=KEY_COLS, how="outer", indicator=True)

    log.info(
        "Key merge value_counts with Measure_Key:\n%s",
        keys_merged["_merge"].value_counts().to_string(),
    )

    keys_only_raw = keys_merged[keys_merged["_merge"] == "left_only"][KEY_COLS].copy()
    keys_only_sem = keys_merged[keys_merged["_merge"] == "right_only"][KEY_COLS].copy()
    keys_in_both = keys_merged[keys_merged["_merge"] == "both"][KEY_COLS].copy()

    log.info("Unique key combos only in Flat_File: %d", len(keys_only_raw))
    log.info("Unique key combos only in Semantic:  %d", len(keys_only_sem))
    log.info("Unique key combos in BOTH:          %d", len(keys_in_both))

    return keys_only_raw, keys_only_sem, keys_in_both


# --------------------------------------------------------------------------------------
# 3) BUILD DIFF TABLE
# --------------------------------------------------------------------------------------

def build_qa_diff(
    flat_for_qa: pd.DataFrame,
    sem_for_qa: pd.DataFrame,
    keys_only_raw: pd.DataFrame,
    keys_only_sem: pd.DataFrame,
    keys_in_both: pd.DataFrame,
    batch_id: str,
    qa_run_datetime: str,
    filtered_excel_files: Optional[Iterable[str]] = None,
    expected_companies: Optional[Iterable[str]] = None,
    status: Optional[str] = None,
    process_cd: Optional[str] = None,
    submission_period_cd: Optional[str] = None,
    target_submission_period: Optional[str] = None,
    logger_: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Build the full QA differences DataFrame.

    Includes:
      - rows only in Flat_File
      - rows only in semantic
      - column-level mismatches for rows in both
      - synthetic "missing company from folder" errors (if inputs provided)

    Parameters
    ----------
    flat_for_qa, sem_for_qa : DataFrame
        Prepared DataFrames from prepare_qa_frames().
    keys_only_raw, keys_only_sem, keys_in_both : DataFrame
        KEY_COLS-only DataFrames from compute_key_overlap().
    batch_id : str
        Batch identifier to stamp onto error rows.
    qa_run_datetime : str
        QA run timestamp as a string.
    filtered_excel_files : iterable of str, optional
        List of Excel file paths in the folder (for missing-company checks).
    expected_companies : iterable of str, optional
        Expected Organisation_Cd values.
    status, process_cd, submission_period_cd, target_submission_period : str, optional
        Extra context for error descriptions.
    logger_ : logging.Logger, optional

    Returns
    -------
    qa_diff_df : DataFrame
        Full error table with one row per discrepancy.
    """
    log = logger_ or logger

    diff_records: list[dict] = []

    # ------------------------------------------------------------------
    # 1) Rows present in Flat_File but missing in ingested
    # ------------------------------------------------------------------
    if not keys_only_raw.empty:
        missing_raw_rows = flat_for_qa.merge(
            keys_only_raw,
            on=KEY_COLS,
            how="inner",
        )
        log.info("Rows present only in Flat_File (Excel): %d", len(missing_raw_rows))

        for _, row in missing_raw_rows.iterrows():
            context = {k: row.get(k) for k in CONTEXT_COLS if k in missing_raw_rows.columns}
            raw_measure = row.get("Measure_Value")
            measure_desc = row.get("Measure_Desc")

            context.update({
                "Error_Type": "MISSING_IN_INGESTED",
                "Column_Name": "Measure_Value",
                "Raw_Value": raw_measure,
                "Ingested_Value": None,
                "Measure_Desc": measure_desc,
                "Error_Desc": (
                    "Row present in Flat_File (Excel) but missing from flattened semantic data "
                    f"for key { {k: row.get(k) for k in KEY_COLS} } "
                    f"(Measure_Cd_raw={row.get('Measure_Cd')!r}). "
                    f"Flat_File Measure_Value={raw_measure!r}."
                ),
            })
            diff_records.append(context)

    # ------------------------------------------------------------------
    # 2) Rows present in ingested but missing in Flat_File
    # ------------------------------------------------------------------
    if not keys_only_sem.empty:
        extra_sem_rows = sem_for_qa.merge(
            keys_only_sem,
            on=KEY_COLS,
            how="inner",
        )
        log.info("Rows present only in Semantic: %d", len(extra_sem_rows))

        for _, row in extra_sem_rows.iterrows():
            context = {k: row.get(k) for k in CONTEXT_COLS if k in extra_sem_rows.columns}
            ing_measure = row.get("Measure_Value")
            measure_desc = row.get("Measure_Desc")

            context.update({
                "Error_Type": "EXTRA_IN_INGESTED",
                "Column_Name": "Measure_Value",
                "Raw_Value": None,
                "Ingested_Value": ing_measure,
                "Measure_Desc": measure_desc,
                "Error_Desc": (
                    "Row present in flattened semantic data but not in Flat_File (Excel) "
                    f"for key { {k: row.get(k) for k in KEY_COLS} } "
                    f"(Measure_Cd_ingested={row.get('Measure_Cd')!r}, "
                    f"Legacy_Measure_Reference={row.get('Legacy_Measure_Reference')!r}). "
                    f"Semantic Measure_Value={ing_measure!r}."
                ),
            })
            diff_records.append(context)

    # ------------------------------------------------------------------
    # 3) Rows present in BOTH: column-level comparisons
    # ------------------------------------------------------------------
    if not keys_in_both.empty:
        left_rows = flat_for_qa.merge(
            keys_in_both,
            on=KEY_COLS,
            how="inner",
        )
        right_rows = sem_for_qa.merge(
            keys_in_both,
            on=KEY_COLS,
            how="inner",
        )

        both = left_rows.merge(
            right_rows,
            on=KEY_COLS,
            suffixes=("_raw", "_ingested"),
            how="inner",
        )

        log.info("Rows present in BOTH (after full join using Measure_Key): %d", len(both))

        common_cols = [
            c for c in COMPARE_COLS
            if f"{c}_raw" in both.columns and f"{c}_ingested" in both.columns
        ]

        def _measure_value_diff_mask(df: pd.DataFrame) -> pd.Series:
            col_raw = "Measure_Value_raw"
            col_ing = "Measure_Value_ingested"

            unit_raw = df["Measure_Unit_raw"] if "Measure_Unit_raw" in df.columns else None
            unit_ing = df["Measure_Unit_ingested"] if "Measure_Unit_ingested" in df.columns else None

            # 1) Normalise to numeric (incl. % handling)
            raw_num = _normalise_measure_value(df[col_raw], unit_raw)
            ing_num = _normalise_measure_value(df[col_ing], unit_ing)

            # Optional debug
            df[col_raw + "_num_original"] = raw_num
            df[col_ing + "_num_original"] = ing_num

            # 2) Handle rows where both sides are effectively missing
            both_na = raw_num.isna() & ing_num.isna()

            # 3) Work out how many decimals to use (take the larger of raw/ingested)
            if "Measure_Decimals_raw" in df.columns:
                dec_raw = pd.to_numeric(df["Measure_Decimals_raw"], errors="coerce")
            else:
                dec_raw = pd.Series(np.nan, index=df.index)

            if "Measure_Decimals_ingested" in df.columns:
                dec_ing = pd.to_numeric(df["Measure_Decimals_ingested"], errors="coerce")
            else:
                dec_ing = pd.Series(np.nan, index=df.index)

            decimals_df = pd.concat([dec_raw, dec_ing], axis=1)
            decimals = decimals_df.max(axis=1)

            default_decimals = 4
            decimals = decimals.fillna(default_decimals).astype(int)
            decimals = decimals.clip(lower=0, upper=10)

            # 4) Round both sides to that precision, per-row
            factor = np.power(10.0, decimals)
            raw_rounded = (raw_num * factor).round().div(factor)
            ing_rounded = (ing_num * factor).round().div(factor)

            df[col_raw + "_num"] = raw_rounded
            df[col_ing + "_num"] = ing_rounded

            equal_rounded = raw_rounded == ing_rounded

            # 5) A diff exists only where:
            #    - not both NaN
            #    - AND rounded values are not equal
            mask = ~(both_na | equal_rounded)
            return mask

        for col in common_cols:
            col_raw = f"{col}_raw"
            col_ing = f"{col}_ingested"

            if col == "Measure_Value":
                mask_diff = _measure_value_diff_mask(both)
                err_type = "MEASURE_VALUE_MISMATCH"

            elif col == "Measure_Decimals":
                raw_num = pd.to_numeric(both[col_raw], errors="coerce")
                ing_num = pd.to_numeric(both[col_ing], errors="coerce")
                mask_diff = ~(
                    (raw_num.isna() & ing_num.isna()) |
                    (raw_num == ing_num)
                )
                err_type = "MEASURE_DECIMALS_MISMATCH"

            else:
                left_norm = _normalise_string(both[col_raw])
                right_norm = _normalise_string(both[col_ing])
                mask_diff = left_norm != right_norm

                if col == "Measure_Desc":
                    err_type = "DESCRIPTION_MISMATCH"
                elif col == "Measure_Unit":
                    err_type = "UNIT_DATATYPE_MISMATCH"
                else:
                    err_type = f"{col.upper()}_MISMATCH"

            diff_rows = both[mask_diff]
            for _, row in diff_rows.iterrows():
                context = {k: row.get(k) for k in CONTEXT_COLS if k in both.columns}
                raw_val = row.get(col_raw)
                ing_val = row.get(col_ing)

                insert_date = row.get("Insert_Date", None)
                measure_cd_raw = row.get("Measure_Cd_raw", None)
                measure_cd_ing = row.get("Measure_Cd_ingested", None)
                legacy_ref = row.get("Legacy_Measure_Reference", None)

                desc = (
                    f"{col} mismatch for key { {k: row.get(k) for k in KEY_COLS} } "
                    f"(Measure_Cd_raw={measure_cd_raw!r}, "
                    f"Measure_Cd_ingested={measure_cd_ing!r}, "
                    f"Legacy_Measure_Reference={legacy_ref!r}, "
                    f"Insert_Date={insert_date!r}): "
                    f"Flat_File={raw_val!r}, Ingested={ing_val!r}."
                )

                measure_desc_raw = row.get("Measure_Desc_raw", None)
                measure_desc_ing = row.get("Measure_Desc_ingested", None)

                try:
                    if pd.notna(measure_desc_raw):
                        measure_desc = measure_desc_raw
                    else:
                        measure_desc = measure_desc_ing
                except Exception:  # pylint: disable=broad-exception-caught
                    measure_desc = measure_desc_raw or measure_desc_ing

                record = {
                    **context,
                    "Error_Type": err_type,
                    "Column_Name": col,
                    "Raw_Value": raw_val,
                    "Ingested_Value": ing_val,
                    "Measure_Desc": measure_desc,
                    "Error_Desc": desc,
                }
                diff_records.append(record)

    # ------------------------------------------------------------------
    # 4) Companies missing from folder-level files (by filename prefix)
    # ------------------------------------------------------------------
    if filtered_excel_files is not None and expected_companies is not None:
        present_orgs_from_files: set[str] = set()
        for path in filtered_excel_files:
            fname = path.split("/")[-1]
            company_prefix = fname.split(" ", 1)[0].strip().upper()
            if company_prefix:
                present_orgs_from_files.add(company_prefix)

        missing_orgs = sorted(set(expected_companies) - present_orgs_from_files)

        if missing_orgs:
            process_str = str(process_cd).upper() if process_cd else None
            missing_list = ", ".join(missing_orgs)
            msg = (
                "Missing companies from folder for "
                f"Status={status}, Process_Cd={process_str}, "
                f"Submission_Period_Cd={submission_period_cd}: {missing_list}"
            )
            log.warning(msg)

            for org in missing_orgs:
                error_desc = (
                    "Missing Company From Folder: "
                    f"Process_Cd={process_str}, "
                    f"Submission_Period_Cd={submission_period_cd}, "
                    f"Status={status}, "
                    f"Organisation_Cd={org}."
                )
                record = {
                    "Organisation_Cd": org,
                    "Region_Cd": None,
                    "Sheet_Cd": None,
                    "Measure_Cd": None,
                    "Legacy_Measure_Reference": None,
                    "Measure_Key": None,
                    "Submission_Period_Cd": target_submission_period,
                    "Observation_Period_Cd": None,
                    "Cell_Cd": None,
                    "Error_Type": "MISSING_COMPANY_FROM_FOLDER",
                    "Column_Name": "Organisation_Cd",
                    "Raw_Value": None,
                    "Ingested_Value": None,
                    "Measure_Desc": None,
                    "Error_Desc": error_desc,
                }
                diff_records.append(record)

    # ------------------------------------------------------------------
    # Build final differences dataframe
    # ------------------------------------------------------------------
    qa_diff_df = pd.DataFrame(diff_records) if diff_records else pd.DataFrame()

    if not qa_diff_df.empty:
        qa_diff_df["Batch_Id"] = batch_id
        qa_diff_df["QA_Run_Datetime"] = qa_run_datetime

    return qa_diff_df


# --------------------------------------------------------------------------------------
# 4) BUILD QA SUMMARY + PER-COMPANY SUMMARY + ERROR COUNTS
# --------------------------------------------------------------------------------------

def build_qa_summaries(
    flat_for_qa: pd.DataFrame,
    sem_for_qa: pd.DataFrame,
    keys_in_both: pd.DataFrame,
    qa_diff_df: pd.DataFrame,
    batch_id: str,
    qa_run_datetime: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build:
      - overall QA summary
      - per-company summary
      - error counts by company & error type
    """
    total_raw_rows = len(flat_for_qa)
    total_ing_rows = len(sem_for_qa)
    rows_with_keys_in_both = len(keys_in_both)

    key_cols_for_diff = [c for c in KEY_COLS if c in qa_diff_df.columns]

    if not qa_diff_df.empty and key_cols_for_diff:
        keys_with_any_error = (
            qa_diff_df[key_cols_for_diff]
            .drop_duplicates()
        )
        total_rows_with_mismatches = len(keys_with_any_error)
        total_matched_rows = rows_with_keys_in_both - total_rows_with_mismatches

        columns_affected = ", ".join(
            sorted(qa_diff_df["Column_Name"].dropna().unique())
        )
        error_types = ", ".join(sorted(qa_diff_df["Error_Type"].unique()))
        total_cell_level_differences = len(qa_diff_df)
    else:
        total_rows_with_mismatches = 0
        total_matched_rows = rows_with_keys_in_both
        columns_affected = ""
        error_types = ""
        total_cell_level_differences = 0

    qa_summary_df = pd.DataFrame([{
        "Batch_Id": batch_id,
        "QA_Run_Datetime": qa_run_datetime,
        "Total_Raw_Rows": total_raw_rows,
        "Total_Ingested_Rows": total_ing_rows,
        "Rows_With_Keys_In_Both": rows_with_keys_in_both,
        "Total_Matched_Rows": total_matched_rows,
        "Total_Rows_With_Mismatches": total_rows_with_mismatches,
        "Total_Cell_Level_Differences": total_cell_level_differences,
        "Columns_Affected": columns_affected,
        "Error_Types": error_types,
    }])

    # ----- Per-company summary -----
    all_orgs = sorted(
        set(flat_for_qa["Organisation_Cd"].unique()).union(
            set(sem_for_qa["Organisation_Cd"].unique())
        )
    )
    qa_company_summary_df = pd.DataFrame({"Organisation_Cd": all_orgs})

    raw_counts_by_org = (
        flat_for_qa
        .groupby("Organisation_Cd")
        .size()
        .reset_index(name="Total_Raw_Rows")
    )

    ing_counts_by_org = (
        sem_for_qa
        .groupby("Organisation_Cd")
        .size()
        .reset_index(name="Total_Ingested_Rows")
    )

    if not keys_in_both.empty:
        keys_in_both_by_org = (
            keys_in_both
            .groupby("Organisation_Cd")
            .size()
            .reset_index(name="Rows_With_Keys_In_Both")
        )
    else:
        keys_in_both_by_org = pd.DataFrame(
            columns=["Organisation_Cd", "Rows_With_Keys_In_Both"]
        )

    if not qa_diff_df.empty and key_cols_for_diff:
        base_for_org = (
            qa_diff_df[key_cols_for_diff]
            .drop_duplicates()
        )

        rows_with_mismatches_by_org = (
            base_for_org
            .groupby("Organisation_Cd")
            .size()
            .reset_index(name="Total_Rows_With_Mismatches")
        )

        cell_diff_by_org = (
            qa_diff_df
            .groupby("Organisation_Cd")
            .size()
            .reset_index(name="Total_Cell_Level_Differences")
        )

        cols_affected_by_org = (
            qa_diff_df
            .groupby("Organisation_Cd")["Column_Name"]
            .apply(
                lambda s: ", ".join(
                    sorted(c for c in s.dropna().unique())
                )
            )
            .reset_index(name="Columns_Affected")
        )

        error_types_by_org = (
            qa_diff_df
            .groupby("Organisation_Cd")["Error_Type"]
            .apply(lambda s: ", ".join(sorted(s.unique())))
            .reset_index(name="Error_Types")
        )
    else:
        rows_with_mismatches_by_org = pd.DataFrame(
            columns=["Organisation_Cd", "Total_Rows_With_Mismatches"]
        )
        cell_diff_by_org = pd.DataFrame(
            columns=["Organisation_Cd", "Total_Cell_Level_Differences"]
        )
        cols_affected_by_org = pd.DataFrame(
            columns=["Organisation_Cd", "Columns_Affected"]
        )
        error_types_by_org = pd.DataFrame(
            columns=["Organisation_Cd", "Error_Types"]
        )

    qa_company_summary_df = (
        qa_company_summary_df
        .merge(raw_counts_by_org, on="Organisation_Cd", how="left")
        .merge(ing_counts_by_org, on="Organisation_Cd", how="left")
        .merge(keys_in_both_by_org, on="Organisation_Cd", how="left")
        .merge(rows_with_mismatches_by_org, on="Organisation_Cd", how="left")
        .merge(cell_diff_by_org, on="Organisation_Cd", how="left")
        .merge(cols_affected_by_org, on="Organisation_Cd", how="left")
        .merge(error_types_by_org, on="Organisation_Cd", how="left")
    )

    for col in [
        "Total_Raw_Rows",
        "Total_Ingested_Rows",
        "Rows_With_Keys_In_Both",
        "Total_Rows_With_Mismatches",
        "Total_Cell_Level_Differences",
    ]:
        if col in qa_company_summary_df.columns:
            qa_company_summary_df[col] = (
                pd.to_numeric(qa_company_summary_df[col], errors="coerce")
                .fillna(0)
                .astype(int)
            )


    qa_company_summary_df["Total_Matched_Rows"] = (
        qa_company_summary_df["Rows_With_Keys_In_Both"]
        - qa_company_summary_df["Total_Rows_With_Mismatches"]
    )

    qa_company_summary_df.insert(0, "Batch_Id", batch_id)
    qa_company_summary_df.insert(1, "QA_Run_Datetime", qa_run_datetime)

    cols_order = [
        "Batch_Id",
        "QA_Run_Datetime",
        "Organisation_Cd",
        "Total_Raw_Rows",
        "Total_Ingested_Rows",
        "Rows_With_Keys_In_Both",
        "Total_Matched_Rows",
        "Total_Rows_With_Mismatches",
        "Total_Cell_Level_Differences",
        "Columns_Affected",
        "Error_Types",
    ]
    qa_company_summary_df = qa_company_summary_df[
        [c for c in cols_order if c in qa_company_summary_df.columns]
    ]

    # ----- Error counts by company & error type -----
    if qa_diff_df.empty:
        error_counts_df = pd.DataFrame(
            columns=[
                "Batch_Id",
                "QA_Run_Datetime",
                "Organisation_Cd",
                "Error_Type",
                "Error_Count",
            ]
        )
    else:
        error_counts_df = (
            qa_diff_df
            .groupby(["Organisation_Cd", "Error_Type"])
            .size()
            .reset_index(name="Error_Count")
            .sort_values(["Organisation_Cd", "Error_Type"])
            .reset_index(drop=True)
        )
        error_counts_df.insert(0, "Batch_Id", batch_id)
        error_counts_df.insert(1, "QA_Run_Datetime", qa_run_datetime)

    return qa_summary_df, qa_company_summary_df, error_counts_df
