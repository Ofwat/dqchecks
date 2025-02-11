import pytest
from unittest import mock
from pyspark.sql import SparkSession
from datetime import datetime

from dqchecks.utils import simple_hdfs_ls

# Mocking the SparkSession and HDFS interaction
@pytest.fixture
def mock_spark():
    with mock.patch.object(SparkSession, 'builder') as mock_builder:
        mock_session = mock.Mock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_session
        yield mock_session

def test_simple_hdfs_ls(mock_spark):
    # Mock the JVM and HDFS file system
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()
    path_mock = mock.Mock()
    status_mock = mock.Mock()
    file_info_mock = mock.Mock()

    # Set the mocks for the file system and paths
    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.globStatus.return_value = [status_mock, status_mock]  # Two files in the directory

    # Mocking file status attributes
    status_mock.getPath.return_value.toString.return_value = "hdfs://example/path/to/file1"
    status_mock.getModificationTime.return_value = 1637170140000  # Mock timestamp in milliseconds

    # Expected file info
    expected_result = [
        {"name": "hdfs://example/path/to/file1", "last_modified": datetime.fromtimestamp(1637170140)}
    ]

    # Call the function and check the result
    result = simple_hdfs_ls("hdfs://example/path/to/")
    assert result == expected_result

def test_empty_directory(mock_spark):
    # Mock an empty directory
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()
    path_mock = mock.Mock()
    status_mock = mock.Mock()

    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.globStatus.return_value = []  # No files in the directory

    # Call the function and check the result
    result = simple_hdfs_ls("hdfs://example/empty/path/")
    assert result == []

def test_invalid_path(mock_spark):
    # Mock the file system to raise an exception for invalid path
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()
    
    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock

    fs_mock.globStatus.side_effect = Exception("Invalid path")

    # Test for invalid path error
    with pytest.raises(Exception):
        simple_hdfs_ls("hdfs://invalid/path/")

def test_incorrect_timestamp_format(mock_spark):
    # Mock a case where the timestamp might be in an incorrect format
    jvm_mock = mock.Mock()
    fs_mock = mock.Mock()
    status_mock = mock.Mock()

    mock_spark.sparkContext._jvm = jvm_mock
    jvm_mock.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.globStatus.return_value = [status_mock]

    # Mock the file status with an incorrect timestamp format
    status_mock.getPath.return_value.toString.return_value = "hdfs://example/path/to/file2"
    status_mock.getModificationTime.return_value = "not_a_timestamp"  # Invalid timestamp format

    # Call the function and check that it can handle invalid timestamps gracefully
    result = simple_hdfs_ls("hdfs://example/path/to/")
    assert result == [{"name": "hdfs://example/path/to/file2", "last_modified": "not_a_timestamp"}]
