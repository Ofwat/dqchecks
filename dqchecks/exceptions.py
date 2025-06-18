"""Exceptions used within the project"""

class EmptyRowsPatternCheckError(Exception):
    """
    Custom exception raised when sheet validation fails due to unexpected non-empty rows.

    This exception is specifically used to indicate that certain sheets do not conform to
    expected formatting rules, such as:
      - Non-empty rows appearing where only empty rows were expected under the header (e.g., row 3).
      - Non-empty cells in the top row (excluding a specified column).

    Attributes:
        under_header_issues (list): List of sheet names with non-empty rows under the header.
        top_row_issues (list): List of sheet names with non-empty top row data.

    Example:
        raise EmptyRowsPatternCheckError(
            under_header_issues=["Sheet1", "Sheet3"],
            top_row_issues=["Sheet2"]
        )
    """
    def __init__(self, under_header_issues, top_row_issues):
        message = "Sheet validation failed."
        details = []
        if under_header_issues:
            details.append(f"Non-empty rows under header in: {under_header_issues}")
        if top_row_issues:
            details.append(f"Non-empty top rows in: {top_row_issues}")
        super().__init__(f"{message} " + " ".join(details))
        self.under_header_issues = under_header_issues
        self.top_row_issues = top_row_issues


class ColumnHeaderValidationError(Exception):
    """
    Raised when a worksheet does not contain the required columns in the correct order.

    Attributes:
        sheet_names (list): Names of sheets with incorrect or missing headers.
        expected_columns (list): The list of expected column headers.
    """
    def __init__(self, sheet_names, expected_columns):
        message = (
            f"Column header validation failed in sheets: {sheet_names}. "
            f"Expected columns (in order and matching case): {expected_columns}"
        )
        super().__init__(message)
        self.sheet_names = sheet_names
        self.expected_columns = expected_columns
