"""
Testing utils.py file
"""

from datetime import datetime
from unittest import mock
import pytest
from pyspark.sql import SparkSession

from dqchecks.utils import simple_hdfs_ls

# Mocking the SparkSession and HDFS interaction
@pytest.fixture
def mock_spark():
    """
    Fixture to mock the SparkSession for testing HDFS interaction.

    This fixture mocks the SparkSession's builder and its `getOrCreate` method
    to return a mocked session. It is used in the tests to simulate SparkSession 
    functionality without actually starting a real session.
    
    Yields:
        mock.Mock: A mock Spark session object.
    """
    with mock.patch.object(SparkSession, 'builder') as mock_builder:
        mock_session = mock.Mock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_session
        yield mock_session

# pylint: disable=W0621
def test_simple_hdfs_ls(mock_spark):
    """
    Test for simple_hdfs_ls function with mocked HDFS interaction.

    This test simulates the case where the HDFS directory contains files. 
    It checks whether the function returns the expected file information, 
    including file paths and modification timestamps.

    Args:
        mock_spark (mock.Mock): A mocked Spark session, injected by pytest fixture.
    """
    # Mock the JVM and HDFS file system
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()
    status_mock = mock.Mock()

    # Set the mocks for the file system and paths
    # pylint: disable=W0212
    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.globStatus.return_value = [status_mock, status_mock]  # Two files in the directory

    # Mocking file status attributes
    status_mock.getPath.return_value.toString.return_value = "hdfs://example/path/to/file1"
    status_mock.getModificationTime.return_value = 1637170140000  # Mock timestamp in milliseconds

    # Expected file info
    expected_result = [
        {"name": "hdfs://example/path/to/file1",
         "last_modified": datetime.fromtimestamp(1637170140)}
    ]

    # Call the function and check the result
    result = simple_hdfs_ls("hdfs://example/path/to/")
    assert result == expected_result

def test_empty_directory(mock_spark):
    """
    Test for simple_hdfs_ls function when the directory is empty.

    This test simulates the case where the HDFS directory is empty. 
    It ensures that the function returns an empty list when no files 
    are present in the directory.

    Args:
        mock_spark (mock.Mock): A mocked Spark session, injected by pytest fixture.
    """
    # Mock an empty directory
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()

    # pylint: disable=W0212
    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.globStatus.return_value = []  # No files in the directory

    # Call the function and check the result
    result = simple_hdfs_ls("hdfs://example/empty/path/")
    assert not result

def test_invalid_path(mock_spark):
    """
    Test for simple_hdfs_ls function when an invalid path is provided.

    This test simulates a situation where an invalid HDFS path is provided.
    It ensures that the function raises an exception when it encounters an invalid path.

    Args:
        mock_spark (mock.Mock): A mocked Spark session, injected by pytest fixture.
    """
    # Mock the file system to raise an exception for invalid path
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()

    # pylint: disable=W0212
    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock

    fs_mock.globStatus.side_effect = Exception("Invalid path")

    # Test for invalid path error
    with pytest.raises(Exception):
        simple_hdfs_ls("hdfs://invalid/path/")

def test_incorrect_timestamp_format(mock_spark):
    """
    Test for simple_hdfs_ls function with incorrect timestamp format.

    This test simulates a situation where the file's modification timestamp is in 
    an incorrect format. It ensures that the function can handle invalid timestamps gracefully.

    Args:
        mock_spark (mock.Mock): A mocked Spark session, injected by pytest fixture.
    """
    # Mock a case where the timestamp might be in an incorrect format
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()
    status_mock = mock.Mock()

    # pylint: disable=W0212
    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.globStatus.return_value = [status_mock]

    # Mock the file status with an incorrect timestamp format
    status_mock.getPath.return_value.toString.return_value = "hdfs://example/path/to/file2"
    status_mock.getModificationTime.return_value = "not_a_timestamp"  # Invalid timestamp format

    # Call the function and check that it can handle invalid timestamps gracefully
    result = simple_hdfs_ls("hdfs://example/path/to/")
    assert result == [{"name": "hdfs://example/path/to/file2", "last_modified": "not_a_timestamp"}]
