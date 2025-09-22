"""
Testing the FileLoader from file_loader.py file
"""
from datetime import datetime, timezone
from unittest.mock import patch
import logging
import os
import pytest
from dqchecks.file_loader import FileLoader

def create_file(path, content="test", mtime=None):
    """Helper to create dummy files with contents and mod time"""
    with open(path, "w", encoding="utf8") as f:
        f.write(content)
    if mtime:
        os.utime(path, (mtime.timestamp(), mtime.timestamp()))

@pytest.fixture
def setup_file_structure(tmp_path):
    """Create directory structure"""
    templates_dir = tmp_path / "Files" / "templates"
    data_collections_dir = tmp_path / "Files" / "data collections"
    templates_dir.mkdir(parents=True)
    data_collections_dir.mkdir(parents=True)

    # Create template file
    template_path = templates_dir / "org1_process_cd=abc_submission_period_cd=202501.xlsx"
    create_file(template_path,
                content="template content",
                mtime=datetime(2025,1,1,12,0,0,tzinfo=timezone.utc))

    # Create bronze data file
    # pylint: disable=C0301
    bronze_path = data_collections_dir / "org1_process_cd=abc_submission_period_cd=202501_status=complete.xlsx"
    create_file(bronze_path,
                content="bronze content",
                mtime=datetime(2025,1,2,12,0,0,tzinfo=timezone.utc))

    return tmp_path, template_path, bronze_path

# pylint: disable=W0621
def test_load_template_success(setup_file_structure):
    """test_load_template_success"""
    # pylint: disable=W0612
    tmp_path, template_path, bronze_path = setup_file_structure

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
    )
    metadata = loader.run()

    assert metadata.path == str(template_path)
    assert metadata.filename == template_path.name
    assert metadata.template_version == template_path.name
    assert metadata.md5_hash is not None
    assert isinstance(metadata.last_modified, datetime)

# pylint: disable=W0621
def test_load_bronze_success(setup_file_structure):
    """test_load_bronze_success"""
    tmp_path, template_path, bronze_path = setup_file_structure

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=False,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        status="complete",
    )
    metadata = loader.run()

    assert metadata.path == str(bronze_path)
    assert metadata.filename == bronze_path.name
    assert metadata.template_version == template_path.name
    assert metadata.md5_hash is not None
    assert isinstance(metadata.last_modified, datetime)
    assert metadata.template_path.endswith("org1_process_cd=abc_submission_period_cd=202501.xlsx")

def test_strict_validation_error_multiple_templates(tmp_path):
    """Setup with two templates to trigger strict validation error"""
    templates_dir = tmp_path / "Files" / "templates"
    templates_dir.mkdir(parents=True)
    file1 = templates_dir / "org1_process_cd=abc_submission_period_cd=202501.xlsx"
    file2 = templates_dir / "org1_process_cd=abc_submission_period_cd=202501_v2.xlsx"
    create_file(file1)
    create_file(file2)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        strict=True
    )
    with pytest.raises(ValueError, match="Expected exactly 1 template file"):
        loader.run()

def test_no_matching_template_found(tmp_path):
    """Setup with no templates at all"""
    templates_dir = tmp_path / "Files" / "templates"
    templates_dir.mkdir(parents=True)
    data_dir = tmp_path / "Files" / "data collections"
    data_dir.mkdir(parents=True)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd="org1",
    )

    with pytest.raises(FileNotFoundError, match="No matching template files found."):
        loader.run()

def test_no_matching_bronze_found(tmp_path):
    """test_no_matching_bronze_found"""
    templates_dir = tmp_path / "Files" / "templates"
    data_dir = tmp_path / "Files" / "data collections"
    templates_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    # Create one template file
    template_file = templates_dir / "org1_process_cd=abc_submission_period_cd=202501.xlsx"
    create_file(template_file)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=False,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        strict=True
    )
    with pytest.raises(FileNotFoundError, match="No matching bronze files found."):
        loader.run()

def test_bronze_no_matching_template_found(tmp_path):
    """Setup bronze file without template file to test error"""
    data_dir = tmp_path / "Files" / "data collections"
    data_dir.mkdir(parents=True)
    bronze_file = data_dir / "org1_process_cd=abc_submission_period_cd=202501_status=complete.xlsx"
    create_file(bronze_file)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=False,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        status="complete",
        strict=False
    )
    # pylint: disable=C0301
    with pytest.raises(FileNotFoundError, match="No matching template files found for the bronze file."):
        loader.run()

def test_filtering_by_conditions(tmp_path):
    """Setup files to test filtering logic"""
    templates_dir = tmp_path / "Files" / "templates"
    templates_dir.mkdir(parents=True)

    # Matching template file
    matching_file = templates_dir / "org1_process_cd=abc_submission_period_cd=202501.xlsx"
    create_file(matching_file)
    # Non-matching template file
    other_file = templates_dir / "org1_process_cd=def_submission_period_cd=202501.xlsx"
    create_file(other_file)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        strict=True
    )
    metadata = loader.run()

    assert metadata.path == str(matching_file)

def test_log_file_stats_exception_branch(tmp_path, caplog):
    """test_log_file_stats_exception_branch"""
    # Setup
    dummy_file = tmp_path / "ghost.xlsx"
    dummy_file.touch()
    loader = FileLoader(source_data_path=str(tmp_path), load_template=True)

    # Simulate that os.stat raises an exception when called
    with patch("os.stat", side_effect=OSError("Permission denied")):
        with caplog.at_level(logging.WARNING):
            loader.log_file_stats([str(dummy_file)], label="Test Files")

    # Check that the warning was logged
    assert any("Could not get file stats" in message for message in caplog.messages)

def test_filter_files_by_org_applies_filter(tmp_path):
    """test_filter_files_by_org_applies_filter"""
    # Arrange
    org_code = "org1"
    matching_file = tmp_path / "org1_submission.xlsx"
    non_matching_file = tmp_path / "other_submission.xlsx"

    matching_file.touch()
    non_matching_file.touch()

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd=org_code,
    )

    # Act
    result = loader.filter_files_by_org(
        files=[str(matching_file), str(non_matching_file)],
        restrict_to_org=True,
    )

    # Assert
    assert len(result) == 1
    assert matching_file.name in result[0]

def test_filter_files_by_prefix_no_org_cd_returns_all(tmp_path):
    """test_filter_files_by_prefix_no_org_cd_returns_all"""
    # Arrange
    file1 = tmp_path / "companyA_data.xlsx"
    file2 = tmp_path / "companyB_data.xlsx"

    file1.touch()
    file2.touch()

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        # No organisation_cd provided
    )

    files = [str(file1), str(file2)]

    # Act
    result = loader.filter_files_by_prefix(files)

    # Assert
    assert result == files

def test_calculate_md5_file_not_found(tmp_path, caplog):
    """test_calculate_md5_file_not_found"""
    # Arrange
    fake_file = tmp_path / "nonexistent.xlsx"

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
    )

    # Act
    with caplog.at_level("WARNING"):
        result = loader.calculate_md5(str(fake_file))

    # Assert
    assert result is None
    assert "Could not calculate MD5" in caplog.text
    assert str(fake_file) in caplog.text

def test_multiple_templates_non_strict(tmp_path):
    """test_multiple_templates_non_strict"""
    templates_dir = tmp_path / "Files" / "templates"
    templates_dir.mkdir(parents=True)
    file1 = templates_dir / "org1_process_cd=abc_submission_period_cd=202501.xlsx"
    file2 = templates_dir / "org1_process_cd=abc_submission_period_cd=202501_v2.xlsx"
    create_file(file1)
    create_file(file2)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        strict=False
    )
    metadata = loader.run()

    assert metadata.filename in [file1.name, file2.name]

def test_no_filters_logs_warning(tmp_path, caplog):
    """test_no_filters_logs_warning"""
    templates_dir = tmp_path / "Files" / "templates"
    templates_dir.mkdir(parents=True)
    file = templates_dir / "some_template.xlsx"
    create_file(file)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        strict=False
    )

    with caplog.at_level("WARNING"):
        loader.run()

    assert "No filters applied to template file search." in caplog.text

def test_filter_files_by_conditions_case_insensitive(tmp_path):
    """test_filter_files_by_conditions_case_insensitive"""
    f1 = tmp_path / "ORG1_PROCESS_CD=ABC_submission_period_cd=202501.xlsx"
    f2 = tmp_path / "irrelevant.xlsx"
    create_file(f1)
    create_file(f2)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=True,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501"
    )

    result = loader.filter_files_by_conditions(
        [str(f1), str(f2)],
        {"process_cd": "ABC", "submission_period_cd": "202501"}
    )

    assert str(f1) in result
    assert str(f2) not in result

def test_template_version_missing_for_bronze(tmp_path):
    """Bronze exists, but no template"""
    data_dir = tmp_path / "Files" / "data collections"
    data_dir.mkdir(parents=True)
    bronze_file = data_dir / "org1_process_cd=abc_submission_period_cd=202501_status=complete.xlsx"
    create_file(bronze_file)

    loader = FileLoader(
        source_data_path=str(tmp_path),
        load_template=False,
        organisation_cd="org1",
        process_cd="abc",
        submission_period_cd="202501",
        status="complete",
        strict=False
    )

    # pylint: disable=C0301
    with pytest.raises(FileNotFoundError, match="No matching template files found for the bronze file."):
        loader.run()
