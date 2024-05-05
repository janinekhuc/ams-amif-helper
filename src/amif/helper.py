title = "amif_helper_func.py"
catalog_name = 'dmpf_dev'
source_schema = 'bronze'

data_dict = {
    'chartofaccounts': {
        'primary_key': ['ChartOfAccounts']
    },
    'chartofaccountstext': {
        'primary_key': ['ChartOfAccounts', 'Language']
    },
    'commitmentitem': {
        'primary_key': ['FinancialManagementArea', 'FinMgmtAreaFiscalYear', 'CommitmentItem', 'ValidityStartDate']
    }
}


def get_cdc_source(table_name: str) -> str:
    """Returns the CDC source name based on the given table name.

    Parameters
    ----------
    table_name : str
        The name of the table for which to generate the CDC source name.

    Returns
    -------
    str
        str: The CDC source name.
    """
    return f'{catalog_name}.{source_schema}.afis_{table_name}'


def get_joined_primary_keys(data_dict: dict, table_name: str) -> str:
    """Joins the primary keys of a given table into a comma-separated string.

    Parameters
    ----------
    data_dict : dict
        The dictionary containing the table's primary key.
    table_name : str
        The name of the table for which to generate the primary key string.

    Returns
    -------
    str
        A comma-separated string of the table's primary key.
    """
    return ",".join(data_dict[table_name]['primary_key'])


def get_cdc_input_arguments(data_dict: dict, table_name: str):
    """Returns a tuple containing the CDC source name and the joined primary keys for a given table.

    Parameters
    ----------
    data_dict : _type_
        The dictionary containing the table's primary key.
    table_name : _type_
        The name of the table for which to generate the CDC input arguments.

    Returns
    -------
    tuple
        A tuple containing the CDC source name and the joined primary keys.
    """
    return get_cdc_source(table_name), get_joined_primary_keys(data_dict, table_name)

# Example usage
# cdc_get_operations(get_cdc_input_arguments(data_dict, 'table_name1'), {full_refresh}, True)
# cdc_get_operations(get_cdc_input_arguments(data_dict, 'table_name2'), {full_refresh}, True)
