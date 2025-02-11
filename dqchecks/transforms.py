import pandas as pd
import datetime

def process_fout_sheets(xlfile, org_cd, submission_period_cd, process_cd, template_version, last_modified):
    """
    Processes all sheets in the given Excel file that start with 'fOut_'.

    Args:
        xlfile (pd.ExcelFile): The Excel file object to process.
        org_cd (str): The organization code to add as a column.
        submission_period_cd (str): The submission period code to add as a column.
        process_cd (str): The process code to add as a column.
        template_version (str): The template version to add as a column.
        last_modified (datetime): The last modified timestamp to add as a column.

    Returns:
        pd.DataFrame: The processed DataFrame after all transformations.

    Raises:
        ValueError: If any of the input values are invalid.
        TypeError: If the provided Excel file is not of the correct type.
        Exception: For any other unexpected errors.
    """

    # Input checks
    if not isinstance(xlfile, pd.ExcelFile):
        raise TypeError("The 'xlfile' argument must be a valid pd.ExcelFile object.")
    
    # Check if required input parameters are of valid type
    if not isinstance(org_cd, str) or not org_cd:
        raise ValueError("The 'org_cd' argument must be a non-empty string.")
    if not isinstance(submission_period_cd, str) or not submission_period_cd:
        raise ValueError("The 'submission_period_cd' argument must be a non-empty string.")
    if not isinstance(process_cd, str) or not process_cd:
        raise ValueError("The 'process_cd' argument must be a non-empty string.")
    if not isinstance(template_version, str) or not template_version:
        raise ValueError("The 'template_version' argument must be a non-empty string.")
    if not isinstance(last_modified, datetime.datetime):
        raise ValueError("The 'last_modified' argument must be a datetime object.")

    try:
        # Filter sheets that start with 'fOut_'
        fout_sheets = [sheet for sheet in xlfile.sheet_names if sheet.startswith("fOut_")]

        # If no matching sheets are found, raise an error
        if not fout_sheets:
            raise ValueError(f"No fOut_* sheets found. Available sheets: {xlfile.sheet_names}")

        # Read matching sheets into DataFrames, skip the first row, and use the second row as header
        df_list = [
            xlfile.parse(sheet, header=1).assign(Sheet_Cd=sheet) 
            for sheet in fout_sheets
        ]

        # Drop rows that are completely NaN (ignoring the 'Sheet_Cd' column)
        df_list = [df.dropna(how='all', subset=df.columns.difference(['Sheet_Cd'])) for df in df_list]

        # If no valid rows remain after dropping NaN rows, raise an error
        if any(i.empty for i in df_list):
            raise ValueError("No valid data found after removing rows with NaN values.")

        # Concatenate all DataFrames into one large DataFrame
        big_df = pd.concat(df_list, ignore_index=True)

        # Identify columns related to observation periods (i.e., columns with a yyyy-yy pattern)
        observation_period_columns = set(big_df.filter(regex=r'^\s*2[0-9]{3}-[1-9][0-9]\s*$').columns.tolist())
        
        # If no observation period columns are found, raise a warning
        if not observation_period_columns:
            raise ValueError("No observation period columns found in the data.")

        # Get ID columns (all columns except observation period columns)
        id_columns = set(big_df.columns.tolist()) - observation_period_columns
        
        # Pivot the DataFrame to melt the observation period columns into rows
        pivoted_df = big_df.melt(id_vars=id_columns, var_name="Observation_Period_Cd", value_name="Measure_Value")

        # Add static columns
        pivoted_df["Organisation_Cd"] = org_cd
        pivoted_df["Submission_Period_Cd"] = submission_period_cd
        pivoted_df["Process_Cd"] = process_cd
        pivoted_df["Template_Version"] = template_version
        pivoted_df["Submission_Date"] = last_modified  # Last modified date

        # Convert all columns to string type for consistency
        pivoted_df = pivoted_df.astype(str)

        # Define column renaming mapping
        column_rename_map = {
            'Organisation_Cd': 'Organisation_Cd',
            'Submission_Period_Cd': 'Submission_Period_Cd',
            'Observation_Period_Cd': 'Observation_Period_Cd',
            'Process_Cd': 'Process_Cd',
            'Template_Version': 'Template_Version',
            'Sheet_Cd': 'Sheet_Cd',
            'Reference': 'Measure_Cd',  # Ensure 'Reference' column exists after melt
            'Measure_Value': 'Measure_Value',
            'Item description': 'Measure_Desc',  # Ensure 'Item Description' column exists after melt
            'Unit': 'Measure_Unit',
            'Model': 'Model_Cd',
            'Submission_Date': 'Submission_Date'
        }

        # Rename columns using the defined mapping
        pivoted_df = pivoted_df.rename(columns=column_rename_map)

        # Reorder columns according to the desired order
        ordered_columns = list(column_rename_map.values())
        pivoted_df = pivoted_df[ordered_columns]

        return pivoted_df

    except Exception as e:
        raise Exception(f"An error occurred while processing the Excel sheets: {str(e)}")