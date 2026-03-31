"""
dqchecks.qa

Reusable QA logic for comparing Flat_File (Excel-derived) data
against semantic / ingested data.

Supports multiple "profiles" so we can reuse the same orchestration
across datasets with different grains/keys/column sets.

Profiles:
- "QD"  (default): original Quarterly Data logic (Measure_Key / Region_Cd / Legacy_Measure_Reference)
- "CCP": Cost Change Process logic (multi-attribute natural key, no Measure_Key)
- "MEX": DMeX / MEX logic (multi-attribute natural key, no Measure_Key)

Designed to be called from Fabric notebooks, e.g.:

    from dqchecks import qa

    flat_for_qa, sem_for_qa = qa.prepare_qa_frames(..., profile="QD")
    keys_only_raw, keys_only_sem, keys_in_both = qa.compute_key_overlap(..., profile="QD")
    qa_diff_df = qa.build_qa_diff(..., profile="QD")
    qa_summary_df, qa_company_summary_df, error_counts_df = qa.build_qa_summaries(..., profile="QD")

This module is intentionally pure-pandas (no Fabric / Spark / DB engine).
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional, Tuple

import pandas as pd

# We intentionally expose orchestration-style functions that take several arguments
# and have branching logic. Suppress corresponding structural warnings.
# Also relax line length and superfluous-parens for readability in f-strings.
# pylint: disable=too-many-arguments,too-many-positional-arguments
# pylint: disable=too-many-locals,too-many-branches,too-many-statements
# pylint: disable=line-too-long,superfluous-parens

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# QD COLUMN CONSTANTS (kept compatible with existing tests + QD notebooks)
# --------------------------------------------------------------------------------------

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

# Composite key to define "same row" for QD
# NOTE: we join using Measure_Key, which is:
#   - Flat_File: Measure_Cd
#   - Semantic:  Legacy_Measure_Reference
KEY_COLS: list[str] = [
    "Organisation_Cd",
    "Region_Cd",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Measure_Key",
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
# CCP COLUMN CONSTANTS
# --------------------------------------------------------------------------------------

CCP_COMPARE_COLS: list[str] = [
    "Organisation_Cd",
    "Assurance_Cd",
    "Observation_Cd",
    "Sensitivity_Cd",
    "Data_Source_Cd",
    "Measure_Cd",
    "Measure_Value",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Observation_Coverage_Cd",
    "Comment",  # Excel Audit_Comment -> Comment
    "Adjustment_Period_Cd",
    "Gated_Scheme_Cd",
    "Major_Project_Cd",
    "Asset_Class_Cd",
    "Site_Cd",
    "Cost_Change_Category_Cd",
    "Inflation_Observation_Cd",
    "Price_Index_Cd",
    "Price_Index_Coverage_Cd",
    "Cost_Claim_Change_Cd",
    "Business_Unit_Cd",
]

CCP_KEY_COLS: list[str] = [
    "Organisation_Cd",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Measure_Cd",
    "Observation_Cd",
    "Data_Source_Cd",
    "Adjustment_Period_Cd",
    "Gated_Scheme_Cd",
    "Major_Project_Cd",
    "Cost_Change_Category_Cd",
    "Cost_Claim_Change_Cd",
    "Business_Unit_Cd",
]

CCP_CONTEXT_COLS: list[str] = CCP_KEY_COLS[:]  # context == key for CCP

# --------------------------------------------------------------------------------------
# MEX COLUMN CONSTANTS
# --------------------------------------------------------------------------------------

MEX_COMPARE_COLS: list[str] = [
    "Organisation_Cd",
    "Assurance_Cd",
    "Measure_Cd",
    "Measure_Value",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Sensitivity_Cd",
    "Observation_Cd",
    "Observation_Coverage_Cd",
    "Data_Source_Cd",
    "DMeX_Metric_Cd",
    "Comment",
    "Process_Cd",
    "Filename",
    "Template_Version",
    "Sheet_Cd",
    "Submission_Date",
]

MEX_KEY_COLS: list[str] = [
    "Organisation_Cd",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Measure_Cd",
    "Observation_Cd",
    "Observation_Coverage_Cd",
    "Data_Source_Cd",
    "Sensitivity_Cd",
    "DMeX_Metric_Cd",
]

MEX_CONTEXT_COLS: list[str] = [
    "Organisation_Cd",
    "Submission_Period_Cd",
    "Observation_Period_Cd",
    "Measure_Cd",
    "Observation_Cd",
    "Observation_Coverage_Cd",
    "Data_Source_Cd",
    "Sensitivity_Cd",
    "DMeX_Metric_Cd",
    "Filename",
    "Sheet_Cd",
]

# --------------------------------------------------------------------------------------
# PROFILE SELECTOR
# --------------------------------------------------------------------------------------

def _profile_name(profile: str | None) -> str:
    return (profile or "QD").strip().upper()


def _get_profile_cols(profile: str | None):
    """
    Returns (compare_cols, key_cols, context_cols) for the selected profile.
    """
    p = _profile_name(profile)
    if p == "CCP":
        return CCP_COMPARE_COLS, CCP_KEY_COLS, CCP_CONTEXT_COLS
    if p == "MEX":
        return MEX_COMPARE_COLS, MEX_KEY_COLS, MEX_CONTEXT_COLS
    # default: QD
    return COMPARE_COLS, KEY_COLS, CONTEXT_COLS


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
    QD-style: Normalise key columns and build Measure_Key from the specified measure_col.
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

    if measure_col not in df.columns:
        raise ValueError(f"{measure_col} not found when building Measure_Key.")

    df["Measure_Key"] = df[measure_col].astype(str).str.strip()
    return df


def _normalise_key_cols(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    """
    CCP/MEX-style: Normalise key columns only (string trim), no Measure_Key building.
    """
    df = df.copy()
    for c in key_cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )
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
      - fill NaN with ''
      - remove zero width space (U+200B)
      - remove literal "\\u200b" text (sometimes appears when pasted/escaped)
      - normalise unicode hyphens/dashes to ASCII '-'
      - strip spaces
      - lowercase
    """
    dash_map = {
        "\u2010": "-",  # hyphen
        "\u2011": "-",  # non-breaking hyphen
        "\u2012": "-",  # figure dash
        "\u2013": "-",  # en dash
        "\u2014": "-",  # em dash
        "\u2212": "-",  # minus sign
    }

    def clean(x) -> str:
        txt = "" if x is None else str(x)

        # remove real ZWSP + literal text "\u200b" if present
        txt = txt.replace("\u200b", "").replace(r"\u200b", "")

        # normalise dash-like chars
        for k, v in dash_map.items():
            txt = txt.replace(k, v)

        return txt.strip().lower()

    return s.fillna("").map(clean)


def _apply_ccp_semantic_renames(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make CCP semantic output align with CCP raw naming conventions.
    Safe: only renames columns if present.
    """
    df = df.copy()
    rename_map = {
        "measure_value": "Measure_Value",
        "Cost_Change_Claim_Cd": "Cost_Claim_Change_Cd",
        "Audit_Comment": "Comment",
    }
    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


def _apply_mex_semantic_renames(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make MEX semantic output align with MEX raw naming conventions.
    Safe: only renames columns if present.
    """
    df = df.copy()
    rename_map = {
        "measure_value": "Measure_Value",
        "Measure_Comment": "Comment",
        "measure_comment": "Comment",
        "FileName": "Filename",
        "file_name": "Filename",
    }
    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


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
    profile: str = "QD",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prepare Flat_File and semantic DataFrames for QA.

    QD:
      - Renames semantic columns to align with Flat_File.
      - Normalises period codes.
      - Filters by Submission_Period_Cd (and optionally Organisation_Cd).
      - Builds Measure_Key:
            Flat_File: Measure_Cd
            Semantic:  Legacy_Measure_Reference
      - Dedupes semantic by latest Insert_Date per KEY_COLS.

    CCP:
      - Applies CCP semantic renames (measure_value -> Measure_Value etc.)
      - Normalises period codes.
      - Filters by Submission_Period_Cd (and optionally Organisation_Cd).
      - Normalises CCP_KEY_COLS only (no Measure_Key / no Legacy_Measure_Reference).
      - Dedupes semantic by latest Insert_Date per CCP_KEY_COLS.

    MEX:
      - Applies MEX semantic renames (measure_value -> Measure_Value etc.)
      - Normalises period codes.
      - Filters by Submission_Period_Cd (and optionally Organisation_Cd).
      - Normalises MEX_KEY_COLS only (no Measure_Key / no Legacy_Measure_Reference).
      - Dedupes semantic by latest Insert_Date per MEX_KEY_COLS.

    Returns
    -------
    (flat_for_qa, sem_for_qa)
    """
    log = logger_ or logger
    p = _profile_name(profile)
    _compare_cols, key_cols, _context_cols = _get_profile_cols(p)

    # 1) Prepare semantic + raw copies
    if p == "QD":
        col_map = semantic_to_flat_map or SEMANTIC_TO_FLAT_COL_MAP
        sem_for_qa = ingested_df_flat.rename(columns=col_map).copy()
    elif p == "CCP":
        sem_for_qa = _apply_ccp_semantic_renames(ingested_df_flat)
    elif p == "MEX":
        sem_for_qa = _apply_mex_semantic_renames(ingested_df_flat)
    else:
        raise ValueError(f"Unsupported profile: {p}")

    flat_for_qa = combined_df.copy()

    # 2) Normalise period codes BEFORE filtering
    flat_for_qa = _normalise_period_codes(flat_for_qa)
    sem_for_qa = _normalise_period_codes(sem_for_qa)

    # 3) Filter by submission period (and optionally org)
    if "Submission_Period_Cd" not in flat_for_qa.columns:
        raise ValueError("Submission_Period_Cd missing from Flat_File input.")
    if "Submission_Period_Cd" not in sem_for_qa.columns:
        raise ValueError("Submission_Period_Cd missing from semantic input.")

    flat_for_qa = flat_for_qa[flat_for_qa["Submission_Period_Cd"] == target_submission_period]
    sem_for_qa = sem_for_qa[sem_for_qa["Submission_Period_Cd"] == target_submission_period]

    if target_org is not None:
        if "Organisation_Cd" in flat_for_qa.columns:
            flat_for_qa = flat_for_qa[flat_for_qa["Organisation_Cd"] == target_org]
        if "Organisation_Cd" in sem_for_qa.columns:
            sem_for_qa = sem_for_qa[sem_for_qa["Organisation_Cd"] == target_org]

    log.info("Flat_File rows BEFORE key normalisation: %d", len(flat_for_qa))
    log.info("Semantic rows BEFORE key normalisation: %d", len(sem_for_qa))

    # 4) Normalise keys
    if p == "QD":
        flat_for_qa = _normalise_keys_with_measure(flat_for_qa, measure_col="Measure_Cd")
        sem_for_qa = _normalise_keys_with_measure(sem_for_qa, measure_col="Legacy_Measure_Reference")
    else:
        flat_for_qa = _normalise_key_cols(flat_for_qa, key_cols)
        sem_for_qa = _normalise_key_cols(sem_for_qa, key_cols)

    log.info("Flat_File rows AFTER key normalisation: %d", len(flat_for_qa))
    log.info("Semantic rows AFTER key normalisation: %d", len(sem_for_qa))

    # 5) Dedupe semantic by latest Insert_Date per key
    if "Insert_Date" in sem_for_qa.columns and not sem_for_qa.empty:
        sem_for_qa["_Insert_Date_ts"] = pd.to_datetime(sem_for_qa["Insert_Date"], errors="coerce")
        sem_for_qa = sem_for_qa.sort_values(
            key_cols + ["_Insert_Date_ts"],
            ascending=[True] * len(key_cols) + [False],
        )
        sem_for_qa = sem_for_qa.drop_duplicates(subset=key_cols, keep="first")
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
    profile: str = "QD",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compute key-level overlap between Flat_File and semantic data.

    Returns three DataFrames containing key columns only:
    - keys_only_raw
    - keys_only_sem
    - keys_in_both
    """
    log = logger_ or logger
    p = _profile_name(profile)
    _compare_cols, key_cols, _context_cols = _get_profile_cols(p)

    raw_keys = flat_for_qa[key_cols].drop_duplicates().copy()
    raw_keys["_side"] = "Flat_File"

    sem_keys = sem_for_qa[key_cols].drop_duplicates().copy()
    sem_keys["_side"] = "Semantic"

    keys_merged = raw_keys.merge(sem_keys, on=key_cols, how="outer", indicator=True)

    log.info("Key merge value_counts:\n%s", keys_merged["_merge"].value_counts().to_string())

    keys_only_raw = keys_merged[keys_merged["_merge"] == "left_only"][key_cols].copy()
    keys_only_sem = keys_merged[keys_merged["_merge"] == "right_only"][key_cols].copy()
    keys_in_both = keys_merged[keys_merged["_merge"] == "both"][key_cols].copy()

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
    profile: str = "QD",
) -> pd.DataFrame:
    """
    Build the full QA differences DataFrame.

    Includes:
      - rows only in Flat_File
      - rows only in semantic
      - column-level mismatches for rows in both
      - synthetic "missing company from folder" errors (if inputs provided)
    """
    log = logger_ or logger
    p = _profile_name(profile)
    compare_cols, key_cols, context_cols = _get_profile_cols(p)

    diff_records: list[dict] = []

    # ------------------------------------------------------------------
    # 1) Rows present in Flat_File but missing in ingested
    # ------------------------------------------------------------------
    if not keys_only_raw.empty:
        missing_raw_rows = flat_for_qa.merge(keys_only_raw, on=key_cols, how="inner")
        log.info("Rows present only in Flat_File: %d", len(missing_raw_rows))

        for _, row in missing_raw_rows.iterrows():
            context = {k: row.get(k) for k in context_cols if k in missing_raw_rows.columns}
            raw_measure = row.get("Measure_Value")
            measure_desc = row.get("Measure_Desc")  # may be None for CCP/MEX

            context.update({
                "Error_Type": "MISSING_IN_INGESTED",
                "Column_Name": "Measure_Value",
                "Raw_Value": raw_measure,
                "Ingested_Value": None,
                "Measure_Desc": measure_desc,
                "Error_Desc": (
                    "Row present in Flat_File but missing from semantic data "
                    f"for key { {k: row.get(k) for k in key_cols} }. "
                    f"Flat_File Measure_Value={raw_measure!r}."
                ),
            })
            diff_records.append(context)

    # ------------------------------------------------------------------
    # 2) Rows present in ingested but missing in Flat_File
    # ------------------------------------------------------------------
    if not keys_only_sem.empty:
        extra_sem_rows = sem_for_qa.merge(keys_only_sem, on=key_cols, how="inner")
        log.info("Rows present only in Semantic: %d", len(extra_sem_rows))

        for _, row in extra_sem_rows.iterrows():
            context = {k: row.get(k) for k in context_cols if k in extra_sem_rows.columns}
            ing_measure = row.get("Measure_Value")
            measure_desc = row.get("Measure_Desc")

            context.update({
                "Error_Type": "EXTRA_IN_INGESTED",
                "Column_Name": "Measure_Value",
                "Raw_Value": None,
                "Ingested_Value": ing_measure,
                "Measure_Desc": measure_desc,
                "Error_Desc": (
                    "Row present in semantic data but not in Flat_File "
                    f"for key { {k: row.get(k) for k in key_cols} }. "
                    f"Semantic Measure_Value={ing_measure!r}."
                ),
            })
            diff_records.append(context)

    # ------------------------------------------------------------------
    # 3) Rows present in BOTH: column-level comparisons
    # ------------------------------------------------------------------
    if not keys_in_both.empty:
        left_rows = flat_for_qa.merge(keys_in_both, on=key_cols, how="inner")
        right_rows = sem_for_qa.merge(keys_in_both, on=key_cols, how="inner")

        both = left_rows.merge(
            right_rows,
            on=key_cols,
            suffixes=("_raw", "_ingested"),
            how="inner",
        )

        log.info("Rows present in BOTH (after full join): %d", len(both))

        common_cols = [
            c for c in compare_cols
            if f"{c}_raw" in both.columns and f"{c}_ingested" in both.columns
        ]

        def _measure_value_diff_mask_qd(df: pd.DataFrame) -> pd.Series:
            col_raw = "Measure_Value_raw"
            col_ing = "Measure_Value_ingested"

            unit_raw = df["Measure_Unit_raw"] if "Measure_Unit_raw" in df.columns else None
            unit_ing = df["Measure_Unit_ingested"] if "Measure_Unit_ingested" in df.columns else None

            raw_num = _normalise_measure_value(df[col_raw], unit_raw)
            ing_num = _normalise_measure_value(df[col_ing], unit_ing)

            df[col_raw + "_num_original"] = raw_num
            df[col_ing + "_num_original"] = ing_num

            both_na = raw_num.isna() & ing_num.isna()
            equal_values = raw_num == ing_num
            return ~(both_na | equal_values)

        def _measure_value_diff_mask_ccp(df: pd.DataFrame) -> pd.Series:
            col_raw = "Measure_Value_raw"
            col_ing = "Measure_Value_ingested"

            raw_num = pd.to_numeric(df[col_raw].astype(str).str.strip().str.replace("%", "", regex=False), errors="coerce")
            ing_num = pd.to_numeric(df[col_ing].astype(str).str.strip().str.replace("%", "", regex=False), errors="coerce")

            both_na = raw_num.isna() & ing_num.isna()
            equal_values = raw_num == ing_num
            return ~(both_na | equal_values)

        for col in common_cols:
            col_raw = f"{col}_raw"
            col_ing = f"{col}_ingested"

            if col == "Measure_Value":
                mask_diff = _measure_value_diff_mask_qd(both) if p == "QD" else _measure_value_diff_mask_ccp(both)
                err_type = "MEASURE_VALUE_MISMATCH"

            elif col == "Measure_Decimals":
                raw_num = pd.to_numeric(both[col_raw], errors="coerce")
                ing_num = pd.to_numeric(both[col_ing], errors="coerce")
                mask_diff = ~((raw_num.isna() & ing_num.isna()) | (raw_num == ing_num))
                err_type = "MEASURE_DECIMALS_MISMATCH"

            else:
                left_norm = _normalise_string(both[col_raw])
                right_norm = _normalise_string(both[col_ing])
                mask_diff = left_norm != right_norm

                # Keep QD test expectations intact
                if col == "Measure_Desc":
                    err_type = "DESCRIPTION_MISMATCH"
                elif col == "Measure_Unit":
                    err_type = "UNIT_DATATYPE_MISMATCH"
                elif col == "Comment":
                    err_type = "COMMENT_MISMATCH"
                else:
                    err_type = f"{col.upper()}_MISMATCH"

            diff_rows = both[mask_diff]
            for _, row in diff_rows.iterrows():
                context = {k: row.get(k) for k in context_cols if k in both.columns}
                raw_val = row.get(col_raw)
                ing_val = row.get(col_ing)

                insert_date = row.get("Insert_Date", None)

                # QD has these; CCP/MEX may not
                measure_cd_raw = row.get("Measure_Cd_raw", None)
                measure_cd_ing = row.get("Measure_Cd_ingested", None)
                legacy_ref = row.get("Legacy_Measure_Reference", None)

                desc = (
                    f"{col} mismatch for key { {k: row.get(k) for k in key_cols} } "
                    f"(Measure_Cd_raw={measure_cd_raw!r}, Measure_Cd_ingested={measure_cd_ing!r}, "
                    f"Legacy_Measure_Reference={legacy_ref!r}, Insert_Date={insert_date!r}): "
                    f"Flat_File={raw_val!r}, Ingested={ing_val!r}."
                )

                measure_desc_raw = row.get("Measure_Desc_raw", None)
                measure_desc_ing = row.get("Measure_Desc_ingested", None)

                # IMPORTANT: keep defensive behavior for unit tests (monkeypatched pd.notna)
                try:
                    if pd.notna(measure_desc_raw):
                        measure_desc = measure_desc_raw
                    else:
                        measure_desc = measure_desc_ing
                except Exception:  # defensive fallback
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
            log.warning(
                "Missing companies from folder for Status=%s, Process_Cd=%s, Submission_Period_Cd=%s: %s",
                status, process_str, submission_period_cd, ", ".join(missing_orgs)
            )

            for org in missing_orgs:
                record = {
                    "Organisation_Cd": org,
                    "Submission_Period_Cd": target_submission_period,
                    "Error_Type": "MISSING_COMPANY_FROM_FOLDER",
                    "Column_Name": "Organisation_Cd",
                    "Raw_Value": None,
                    "Ingested_Value": None,
                    "Measure_Desc": None,
                    "Error_Desc": (
                        "Missing Company From Folder: "
                        f"Process_Cd={process_str}, Submission_Period_Cd={submission_period_cd}, "
                        f"Status={status}, Organisation_Cd={org}."
                    ),
                }
                # add extra context columns if they exist for this profile
                for c in context_cols:
                    record.setdefault(c, None)
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
    profile: str = "QD",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build:
      - overall QA summary
      - per-company summary
      - error counts by company & error type
    """
    p = _profile_name(profile)
    _compare_cols, key_cols, _context_cols = _get_profile_cols(p)

    total_raw_rows = len(flat_for_qa)
    total_ing_rows = len(sem_for_qa)
    rows_with_keys_in_both = len(keys_in_both)

    key_cols_for_diff = [c for c in key_cols if c in qa_diff_df.columns]

    if not qa_diff_df.empty and key_cols_for_diff:
        keys_with_any_error = qa_diff_df[key_cols_for_diff].drop_duplicates()
        total_rows_with_mismatches = len(keys_with_any_error)
        total_matched_rows = rows_with_keys_in_both - total_rows_with_mismatches

        columns_affected = ", ".join(sorted(qa_diff_df["Column_Name"].dropna().unique()))
        error_types = ", ".join(sorted(qa_diff_df["Error_Type"].dropna().unique()))
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
    all_orgs = sorted(set(flat_for_qa["Organisation_Cd"].unique()).union(set(sem_for_qa["Organisation_Cd"].unique())))
    qa_company_summary_df = pd.DataFrame({"Organisation_Cd": all_orgs})

    raw_counts_by_org = flat_for_qa.groupby("Organisation_Cd").size().reset_index(name="Total_Raw_Rows")
    ing_counts_by_org = sem_for_qa.groupby("Organisation_Cd").size().reset_index(name="Total_Ingested_Rows")

    if not keys_in_both.empty and "Organisation_Cd" in keys_in_both.columns:
        keys_in_both_by_org = keys_in_both.groupby("Organisation_Cd").size().reset_index(name="Rows_With_Keys_In_Both")
    else:
        keys_in_both_by_org = pd.DataFrame(columns=["Organisation_Cd", "Rows_With_Keys_In_Both"])

    if not qa_diff_df.empty and key_cols_for_diff and "Organisation_Cd" in qa_diff_df.columns:
        base_for_org = qa_diff_df[key_cols_for_diff].drop_duplicates()

        rows_with_mismatches_by_org = base_for_org.groupby("Organisation_Cd").size().reset_index(name="Total_Rows_With_Mismatches")
        cell_diff_by_org = qa_diff_df.groupby("Organisation_Cd").size().reset_index(name="Total_Cell_Level_Differences")

        cols_affected_by_org = (
            qa_diff_df.groupby("Organisation_Cd")["Column_Name"]
            .apply(lambda s: ", ".join(sorted(c for c in s.dropna().unique())))
            .reset_index(name="Columns_Affected")
        )
        error_types_by_org = (
            qa_diff_df.groupby("Organisation_Cd")["Error_Type"]
            .apply(lambda s: ", ".join(sorted(s.dropna().unique())))
            .reset_index(name="Error_Types")
        )
    else:
        rows_with_mismatches_by_org = pd.DataFrame(columns=["Organisation_Cd", "Total_Rows_With_Mismatches"])
        cell_diff_by_org = pd.DataFrame(columns=["Organisation_Cd", "Total_Cell_Level_Differences"])
        cols_affected_by_org = pd.DataFrame(columns=["Organisation_Cd", "Columns_Affected"])
        error_types_by_org = pd.DataFrame(columns=["Organisation_Cd", "Error_Types"])

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
            qa_company_summary_df[col] = pd.to_numeric(qa_company_summary_df[col], errors="coerce").fillna(0).astype(int)

    if "Rows_With_Keys_In_Both" in qa_company_summary_df.columns and "Total_Rows_With_Mismatches" in qa_company_summary_df.columns:
        qa_company_summary_df["Total_Matched_Rows"] = (
            qa_company_summary_df["Rows_With_Keys_In_Both"]
            - qa_company_summary_df["Total_Rows_With_Mismatches"]
        )
    else:
        qa_company_summary_df["Total_Matched_Rows"] = 0

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
    qa_company_summary_df = qa_company_summary_df[[c for c in cols_order if c in qa_company_summary_df.columns]]

    # ----- Error counts by company & error type -----
    if qa_diff_df.empty:
        error_counts_df = pd.DataFrame(
            columns=["Batch_Id", "QA_Run_Datetime", "Organisation_Cd", "Error_Type", "Error_Count"]
        )
    else:
        error_counts_df = (
            qa_diff_df.groupby(["Organisation_Cd", "Error_Type"])
            .size()
            .reset_index(name="Error_Count")
            .sort_values(["Organisation_Cd", "Error_Type"])
            .reset_index(drop=True)
        )
        error_counts_df.insert(0, "Batch_Id", batch_id)
        error_counts_df.insert(1, "QA_Run_Datetime", qa_run_datetime)

    return qa_summary_df, qa_company_summary_df, error_counts_df