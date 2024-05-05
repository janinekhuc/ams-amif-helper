import pytest
from amif.parse import search_col_without_alias, search_alias_columns_in_case_when, find_all_renames_from_as_clause, get_selected_columns, get_aliases


@pytest.mark.parametrize("part, expected", [
])
def test_extract_from_cast_statement(part, expected):
    assert extract_from_cast_statement(part) == expected


@pytest.mark.parametrize("part, expected", [
])
def test_extract_from_case_when_statement(part, expected):
    assert extract_from_case_when_statement(part) == expected


@pytest.mark.parametrize("part, expected", [
    ("cast(scd.abc as decimal(18,2)) as abcd"), ['abc']
])
def test_search_col_from_cast_statement(part, expected):
    assert search_col_from_cast_statement(part) == expected


@pytest.mark.parametrize("part, expected", [
    (" case when scd.abc is null then...", ["abc"])
])
def test_search_col_from_case_when_statement(part, expected):
    assert search_col_from_case_when_statement(part) == expected


@pytest.mark.parametrize("part, expected", [
    ("scd.abc as abcd", ["scd.abc"])
])
def test_split_col_before_as_clause(part, expected):
    assert split_col_before_as_clause(part) == expected


@pytest.mark.parametrize("part, expected", [
    ("SELECT abc, def FROM table1", ["abc", "def"]),
    ("SELECT scd.abc, scd.def FROM table1", ["abc", "def"]),
    ("SELECT scd.abc as abcd, scd.def as defg FROM table1", ["abc", "def"]),
])
def test_search_col_without_alias(part, expected):
    assert search_col_without_alias(part) == expected


@pytest.mark.parametrize("part, expected", [
    (",case when scd.abc is null then NULL when end as abc", {}),
    (",case when scd.abc is null then NULL when end as abc", {}),
    (",case when scd.abc is null then NULL when end as abcd", {"abc": "abcd"}),
    (", case \nwhen scd.abc is null then NULL when end as abcd",
     {"abc": "abcd"}),
    (", CASE \nwhen scd.abc is null then NULL when end as abcd",
     {"abc": "abcd"}),
    (""", case when scd.abc is null then NULL
            end as abcd
        ,case when scd.abcdef is null then NULL
            end as abc
        ,case
            when scd.def is null then null
            when
                [...]
            else
                [...]
        end as def,
        """, {"abc": "abcd", "abcdef": "abc"})
])
def test_search_alias_columns_in_case_when(part, expected):
    assert search_alias_columns_in_case_when(part) == expected


@pytest.mark.parametrize("sql_statement, expected", [
    ("SELECT scd.abc as abcd, scd.def as defd FROM table1 as scd",
     {"abc": "abc", "def": "defd"}),
    ("select abc as abcd, def as defd", {'abc': 'abcd', 'def': 'defd'}),
    ("case when abc is null then null end as def", {})
])
def test_find_all_renames_from_as_clause(sql_statement, expected):
    aliases = {}
    find_all_renames_from_as_clause(sql_statement, aliases)
    assert aliases == expected


@pytest.mark.parametrize("sql_statement, expected", [
    ("""SELECT
     scd.abc as abc,
     scd.def,
     cast(scd.abcd as decimal(18,2) as abcd_num),
     case when defg is null then NULL ... end as defgh,
     case when ghi is null then NULL ...
     FROM table1
     """,
     ['abc', 'def', 'abcd', 'defg', 'ghi']),
])
def test_get_selected_columns(sql_statement, expected):
    selected_columns = get_selected_columns(sql_statement)
    assert selected_columns == expected
    assert isinstance(selected_columns, list)


@pytest.mark.parametrize("expected", [
    {"scd.abc": "t1.abc", "scd.def": "t1.def"},
])
def test_get_aliases(expected):
    aliases = {}
    parser = SQLParser('test_sql.sql')
    parser.parse()
    assert parser.get_aliases() == expected
