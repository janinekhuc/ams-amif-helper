import pytest
from amif.helper import catalog_name, source_schema, get_cdc_source, get_joined_primary_keys, get_cdc_input_arguments


@pytest.fixture
def data_dict():
    return {
        'chartofaccounts': {'primary_key': ['ChartOfAccounts']},
        'chartofaccountstext': {'primary_key': ['ChartOfAccounts', 'Language']},
        'commitmentitem': {'primary_key': ['FinancialManagementArea', 'FinMgmtAreaFiscalYear', 'CommitmentItem', 'ValidityStartDate']}
    }


def test_get_cdc_source():
    table_name = 'chartofaccounts'
    expected_result = f'{catalog_name}.{source_schema}.afis_{table_name}'
    assert get_cdc_source(table_name) == expected_result


def test_get_joined_primary_keys(data_dict):
    table_name = 'commitmentitem'
    expected_result = ','.join(
        ['FinancialManagementArea', 'FinMgmtAreaFiscalYear', 'CommitmentItem', 'ValidityStartDate'])
    assert get_joined_primary_keys(data_dict, table_name) == expected_result


def test_get_cdc_input_arguments(data_dict):
    table_name = 'chartofaccounts'
    expected_result = (get_cdc_source(table_name),
                       get_joined_primary_keys(data_dict, table_name))
    assert get_cdc_input_arguments(data_dict, table_name) == expected_result
