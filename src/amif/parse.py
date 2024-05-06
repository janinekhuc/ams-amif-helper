import re
from typing import Dict, List

# TODO add more tests, docstrings


def extract_from_cast_statement(part: str, selected_columns: List[str]):
    cast_match = search_col_from_cast_statement(part)
    if cast_match:
        column_name = cast_match.group(1).strip()
        match = search_col_without_alias(column_name)
        if match:
            selected_columns.append(match.group(1))
        else:
            selected_columns.append(column_name)
    return selected_columns


def extract_from_case_when_statement(part: str, selected_columns: List[str]):
    case_match = search_col_from_case_when_statement(part)
    if case_match:
        column_name = case_match.group(0).strip()
        match = search_col_without_alias(column_name)
        if match:
            selected_columns.append(match.group(1))
        else:
            selected_columns.append(column_name)
    return selected_columns


def search_col_from_cast_statement(part: str):
    """extract column name from cast function. E.g.
    cast(scd.TaxAmountInCoCodeCrcy as decimal(18,2)) as belasting_bedrag
    returns TaxAmountInCoCodeCrcy
    """
    column_name = re.search(
        r"\bcast\((.+?)\b(?:\s+as\s+.+)?\)", part, re.IGNORECASE)
    return column_name


def search_col_from_case_when_statement(part: str):
    """extract column name from cast function. E.g.
    " case when scd.TaxReportingDate is null then...
    returns scd.TaxReportingDate
    """
    column_name = re.search(r'(?<=case when )[\w.]+', part, re.IGNORECASE)
    return column_name


def split_col_before_as_clause(part: str):
    """extract column name from cast function. E.g.
    scd.fiscalyear as boekjaar_soort
    returns scd.fiscalyear
    """
    column_name = re.split(r"\s+as\s+", part, flags=re.IGNORECASE)[0].strip()
    return column_name


# renamed columns --------------

def search_col_without_alias(part: str):
    column_name = re.search(r"(?<!\w)(?:\w+\.)?(\w+)(?:\s+as\s+\w+)?", part)
    return column_name


def search_alias_columns_in_case_when(sql_statement: str, aliases: Dict[str, str]) -> Dict[str, str]:
    """Extract name changes if it happens in a case when steatment.
    e.g. ',case when scd.abc [...] end as abc' ->  {},
    while ',case when scd.abc [...] end as abcd' -> {abc:abcd}
    """
    sections = sql_statement.split(r'case')  # split based on "case statement"

    for section in sections:
        match = re.search(r"\s+when\s+(\w+\.\w+)\s+is\s+null\s+then\s+(?:NULL|null)\s+when\b.*?\bend\s+as\s+(\w+)",
                          section, re.IGNORECASE | re.DOTALL)
        if match:
            column_name = match.group(1)
            alias_name = match.group(2)
            # only add if name is different as.abc and abc, should not be shown.
            check_column_name = search_col_without_alias(column_name)
            if check_column_name:
                column_name = check_column_name.group(1)
                if column_name.strip() == alias_name.strip():
                    continue
                else:
                    aliases[column_name.strip()] = alias_name.strip()
            aliases[column_name.strip()] = alias_name.strip()
    return aliases


def find_all_renames_from_as_clause(sql_statement: str, aliases: Dict[str, str]) -> Dict[str, str]:
    """Finds all renames that follow the pattern 'abc as abcd',
    excludes renames from case when, that end with 'end as abcd'
    """
    alias_pattern = r"\b(\w+)\s+as\s+(\w+)\b"
    alias_matches = re.findall(alias_pattern, sql_statement, re.IGNORECASE)
    for match in alias_matches:
        original_column, renamed_column = match
        if (original_column != 'end') and (original_column != renamed_column) and (renamed_column != 'decimal'):  # skip end as for now
            aliases[original_column] = renamed_column
    return aliases


def get_selected_columns(sql_statement: str) -> List[str]:
    select_pattern = r"\bselect\b(.+?)\bfrom\b"
    select_match = re.search(
        select_pattern, sql_statement, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_clause = select_match.group(1)
        selected_columns = []
        # split by comma within select statement
        for part in re.split(r",(?![^()]*\))", select_clause):
            part = part.strip()  # Remove leading/trailing whitespaces
            # Check if there's a cast function or case statement
            if "cast(" in part:
                selected_columns = (extract_from_cast_statement(
                    part, selected_columns))
            elif "case" in part:
                # Extract the column name after the case statement if found
                selected_columns = (extract_from_case_when_statement(
                    part, selected_columns))
            else:
                # Check if there's a prefix followed by a dot
                match = search_col_without_alias(part)
                if match:
                    selected_columns.append(match.group(1))
                else:
                    # Extract the column name before any "as" clause
                    column_name = split_col_before_as_clause(part)
                    selected_columns.append(column_name)
    return selected_columns


def get_alias_columns(sql_statement: str) -> Dict[str, str]:
    aliases = {}
    select_pattern = r"\bselect\b(.+?)\bfrom\b"
    select_match = re.search(
        select_pattern, sql_statement, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_clause = select_match.group(1)
        for part in re.split(r",(?![^()]*\))", select_clause):
            part = part.strip()
            if "cast(" in part:
                column_name = search_col_from_cast_statement(
                    part).group(1).strip()
                alias_name = re.search(
                    r"\s+(.*)as\s+(.*)", part, re.IGNORECASE).group(2).strip()
                aliases[column_name] = alias_name
            elif "case" in part:
                aliases = search_alias_columns_in_case_when(part, aliases)
            else:
                aliases = find_all_renames_from_as_clause(part, aliases)
    return aliases


class SQLParser:
    def __init__(self, sql_file: str):
        self.sql_file = sql_file
        self.aliases: Dict[str, str] = {}
        self.selected_columns: List[str] = []

    def parse(self) -> None:
        with open(self.sql_file, 'r') as file:
            sql_statement = file.read()

        # Find the SELECT statement
        self.selected_columns = get_selected_columns(sql_statement)

        # Find the AS clause (column renames)
        self.aliases = get_alias_columns(sql_statement)

    def get_aliases(self) -> Dict[str, str]:
        return self.aliases

    def get_selected_columns(self) -> List[str]:
        return self.selected_columns


if __name__ == '__main__':
    path = 'xyz'
    parser = SQLParser(path)
    parser.parse()
    column_renames = parser.get_aliases()
    selected_columns = parser.get_selected_columns()
