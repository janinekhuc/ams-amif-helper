import pytest
from amif.utils import (filter_list_by_boolean,
                        check_elements_in_list,
                        check_required_fields,
                        append_delta)
from unittest.mock import MagicMock, patch


def test_filter_list_by_boolean():
    list_to_filter = [1, 2, 3, 4, 5]
    boolean_list = [True, False, True, False, True]
    expected_result = [1, 3, 5]

    result = filter_list_by_boolean(list_to_filter, boolean_list)

    assert result == expected_result, "Filtering by boolean list failed"


def test_check_elements_in_list():
    elements_to_check = [1, 2, 3, 4, 5]
    elements_expected = [3, 5, 7]
    expected_result = [True, True, False]

    result = check_elements_in_list(elements_to_check, elements_expected)

    assert result == expected_result, "Checking elements in list failed"


def test_check_required_fields():
    fields_to_check = ['name', 'age', 'email']
    fields_expected = ['name', 'age', 'email', 'address']

    try:
        check_required_fields(fields_to_check, fields_expected)
    except AssertionError as e:
        assert str(
            e) == "Missing field(s) are ['address']. Fields to check contains ['name', 'age', 'email']", "Checking required fields failed"
    else:
        assert False, "Checking required fields passed unexpectedly"


@patch('pyspark.sql.DataFrameWriter.format')
def test_append_delta(mock_format):
    # Arrange
    df = MagicMock()
    delta_path = "/path/to/delta"
    mock_format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None

    # Act
    append_delta(df, delta_path)

    # Assert
    df.write.format.assert_called_once_with("delta")
    df.write.format().mode.assert_called_once_with("append")
    df.write.format().mode().option.assert_called_once_with("mergeSchema", "true")
    df.write.format().mode().option().saveAsTable.assert_called_once_with(delta_path)
