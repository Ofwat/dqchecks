import pytest
from openpyxl import Workbook
from dqchecks.panacea import validate_tabs_between_spreadsheets

@pytest.fixture
def workbook1():
    wb = Workbook()
    wb.create_sheet("Sheet1")
    wb.create_sheet("Sheet2")
    return wb


@pytest.fixture
def workbook2():
    wb = Workbook()
    wb.create_sheet("Sheet1")
    wb.create_sheet("Sheet2")
    return wb


@pytest.fixture
def workbook3():
    wb = Workbook()
    wb.create_sheet("Sheet1")
    wb.create_sheet("Sheet3")
    return wb


@pytest.fixture
def empty_workbook():
    return Workbook()


def test_same_tabs(workbook1, workbook2):
    """Test case where both workbooks have the same sheet names."""
    assert validate_tabs_between_spreadsheets(workbook1, workbook2) is True


def test_different_tabs(workbook1, workbook3):
    """Test case where the workbooks have different sheet names."""
    assert validate_tabs_between_spreadsheets(workbook1, workbook3) is False


def test_empty_workbook(workbook1, empty_workbook):
    """Test case where one workbook is empty (no sheets)."""
    assert validate_tabs_between_spreadsheets(workbook1, empty_workbook) is False


def test_invalid_type(workbook2):
    """Test case where the argument is not a valid openpyxl workbook."""
    with pytest.raises(ValueError):
        validate_tabs_between_spreadsheets("not_a_workbook", workbook2)

    with pytest.raises(ValueError):
        validate_tabs_between_spreadsheets(workbook2, "not_a_workbook")


def test_invalid_object(workbook2):
    """Test case where the argument is an invalid object."""
    with pytest.raises(ValueError):
        validate_tabs_between_spreadsheets(None, workbook2)

    with pytest.raises(ValueError):
        validate_tabs_between_spreadsheets(workbook2, None)
