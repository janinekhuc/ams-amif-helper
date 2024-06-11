import typing as t


def filter_list_by_boolean(list_to_filter: list, boolean_list: t.List[bool]) -> list:
    """Filter list by boolean list."""
    return [elem for (elem, truth) in zip(list_to_filter, boolean_list) if truth]


def check_elements_in_list(elements_to_check: list, elements_expected: list) -> t.List[bool]:
    """Check if a number of expected elements are in a list as expected."""
    return [elem in elements_to_check for elem in elements_expected]


def _check_required_fields(fields_to_check: list, fields_expected: list):
    """Assert whether required fields are in a given list.

    Parameters
    ----------
    fields_to_check : list
        List of items to check.
    fields_expected : list
        List of expected fields.
    """
    # check if all fields are available as expected, returns boolean list
    checked_fields = check_elements_in_list(fields_to_check, fields_expected)
    # reverse boolean list
    reverse_checked_fields = [not elem for elem in checked_fields]
    missing_fields = filter_list_by_boolean(
        fields_expected, reverse_checked_fields)
    assert all(
        checked_fields), f"Missing field(s) are {missing_fields}. Fields to check contains {fields_to_check}"


def append_delta(df, delta_path):
    return df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f'{delta_path}')
