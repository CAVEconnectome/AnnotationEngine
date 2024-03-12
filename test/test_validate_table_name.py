import pytest
from annotationengine.api import is_valid_table_name

@pytest.mark.parametrize("table_name", [
    "valid_table_name",
    "another_valid_name",
    "a",
    "_valid",
    "valid_",
    "valid_table_1"
])
def test_valid_table_names(table_name):
    is_valid, error_msg = is_valid_table_name(table_name)
    assert is_valid == True
    assert error_msg == ""

@pytest.mark.parametrize("table_name", [
    "InvalidUpperCase",
    "invalid space",
    "invalid-table",
    "invalid.table",
    "@invalid",
    "",
    "123"
])
def test_invalid_table_names(table_name):
    is_valid, error_msg = is_valid_table_name(table_name)
    assert is_valid == False
    if table_name == "":
        assert error_msg == "Table name cannot be empty."
    else:
        assert error_msg == "Invalid table name. Table name must include at least one letter, and can only contain lowercase letters, numbers, and underscores (_)."