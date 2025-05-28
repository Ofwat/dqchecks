"""
Collection of helper functions
"""
import datetime
from pyspark.sql import SparkSession
import pandas as pd

def simple_hdfs_ls(path: str) -> list:
    """
    List files in an HDFS directory and retrieve their last modification time.

    This function interacts with the Hadoop Distributed File System (HDFS) to list the files in a 
    specified directory, along with their last modification timestamp. The function uses PySpark's 
    SparkSession to connect to the HDFS and retrieve file metadata.

    Args:
        path (str): The HDFS path to the directory whose files are to be listed.

    Returns:
        list: A list of dictionaries where each dictionary contains the file path ('name') and 
              the last modification time ('last_modified') of each file in the specified HDFS 
              directory. The 'last_modified' time is converted to a human-readable datetime format.

    Example:
        >>> file_info = simple_hdfs_ls("hdfs://path/to/directory")
        >>> for file in file_info:
                print(f"File: {file['name']}, Last Modified: {file['last_modified']}")
    """
    spark = SparkSession.builder.appName("spark_entry_job").getOrCreate()
    # pylint: disable=W0212
    jvm = spark.sparkContext._jvm
    fs_root = jvm.java.net.URI.create("")
    # pylint: disable=W0212
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_root, conf)

    path_glob = jvm.org.apache.hadoop.fs.Path(path)
    status_list = fs.globStatus(path_glob)

    # Generate a list of tuples with the file path and last modification time
    file_info = []
    for status in status_list:
        file_path = status.getPath().toString()
        last_modified_time = status.getModificationTime()  # Get last modified time in milliseconds

        # Convert last modified time from milliseconds to a readable format
        if isinstance(last_modified_time, (float, int)):
            last_modified_datetime = datetime.datetime.fromtimestamp(
                last_modified_time / 1000.0
            )
        else:
            last_modified_datetime = last_modified_time

        new_val = {"name": file_path, "last_modified": last_modified_datetime}

        if new_val not in file_info:
            file_info.append(new_val)

    return file_info

def create_validation_event_row_dataframe(**kwargs):
    """
    Creates a pandas DataFrame representing a single validation event row.
    
    All known columns are initialized. Columns for which values are not supplied
    will be set to None.

    Parameters:
    **kwargs: Arbitrary keyword arguments corresponding to the column-value pairs
              to populate in the DataFrame. Only predefined column names are allowed.
    
    Returns:
    pd.DataFrame: A pandas DataFrame with one row and all predefined columns.
    
    Raises:
    ValueError: If any of the supplied kwargs do not match the predefined column names.
    
    Example:
    >>> create_validation_event_row_dataframe(Event_Id=123, Error_Desc="Invalid format")
           Event_Id Batch_Id Validation_Processing_Stage ... Error_Desc
    0         123     None                      None     ... Invalid format
    """

    columns = [
        "Event_Id",
        "Batch_Id",
        "Validation_Processing_Stage",
        "Sheet_Cd",
        "Template_Version",
        "Rule_Cd",
        "Organisation_Cd",
        "Measure_Cd",
        "Measure_Unit",
        "Measure_Desc",
        "Submission_Period_Cd",
        "Process_Cd",
        "Error_Category",
        "Section_Cd",
        "Cell_Cd",
        "Data_Column",
        "Error_Value",
        "Error_Severity_Cd",
        "Error_Desc"
    ]

    # Validate input keys
    invalid_keys = set(kwargs.keys()) - set(columns)
    if invalid_keys:
        raise ValueError(
            f"Invalid column names provided: {invalid_keys}. Allowed columns are: {columns}")

    # Create a dictionary with all columns set to None by default, override with kwargs if provided
    data = {col: [kwargs.get(col, None)] for col in columns}

    return pd.DataFrame(data)
