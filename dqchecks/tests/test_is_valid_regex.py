"""
Tests for is_valid_regex from transforms.py file
"""
from dqchecks.transforms import is_valid_regex

# Test valid regex patterns
def test_is_valid_regex_valid_patterns():
    """
    Test case to check if the function returns True for valid regex patterns.
    """
    valid_patterns = [
        r'^\s*2[0-9]{3}-[1-9][0-9]\s*$',  # A simple regex pattern
        r'^[a-zA-Z]+$',  # Pattern for matching letters
        r'^\d{3}-\d{2}-\d{4}$',  # Pattern for matching SSN
        r'.*',  # A pattern that matches any string
        r'\\d+',  # A pattern to match digits with an escape sequence
        r'^[A-Za-z0-9_]*$',  # Pattern to match alphanumeric and underscore
    ]

    for pattern in valid_patterns:
        assert is_valid_regex(pattern), f"Failed for pattern: {pattern}"

# Test invalid regex patterns
def test_is_valid_regex_invalid_patterns():
    """
    Test case to check if the function returns False for invalid regex patterns.
    """
    invalid_patterns = [
        r'^[a-zA-Z+',  # Missing closing bracket
        r'[^',  # Unmatched square bracket
        r'(',  # Unmatched parenthesis
    ]

    for pattern in invalid_patterns:
        assert not is_valid_regex(pattern), f"Failed for pattern: {pattern}"

# Test a pattern that is technically valid but has no match (it should return True)
def test_is_valid_regex_no_match():
    """
    Test case to check that a pattern that is technically valid but has no match is handled.
    """
    assert is_valid_regex(r"^xyz$"), "Pattern should be valid even if it doesn't match any string."
