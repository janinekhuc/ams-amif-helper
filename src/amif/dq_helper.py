import pyspark.sql.functions as F
import pyspark.sql as p
from amif.utils import check_required_fields


def create_dq_body(df: p.DataFrame, dq_meta_dict: dict):
    """Initial data quality body. Create a detailed data quality body adding metadata.

    Parameters
    ----------
    df : p.DataFrame
        Filtered PySpark Dataframe containing all the data quality issues with a _quality_ak column.
    dq_meta_dict : dict
        Dictionary containing metadata information. Format is as follows:
        {
            'ak_keys': ak_keys,
            'schema': catalogue,
            'from_table': table_name,
            'job_id': job_id,
            'run_id': run_id,
            'environment': environment
        }

    Returns
    -------
    p.DataFrame
        PySpark Dataframe containing the data quality issues with additional columns.
    """
    # _check_required_fields(df.columns, dq_meta_dict['ak_keys'])
    quarantine_cols = ['schema', 'from_table', 'job_id', 'run_id', 'environment', '_dt_ingest', '_quality_ak', 'row_sha2',
                       'ak_sha2', 'ak_keys_values', 'ak_keys', 'column_values', 'column_names']
    detailed_quarantine = (df
                           .withColumn('column_values', F.split(F.concat_ws(',', *df.columns), ','))
                           .withColumn("column_names", F.split(F.lit(','.join(df.columns)), ','))
                           .withColumn('ak_keys_values', F.split(F.concat_ws(',', *dq_meta_dict['ak_keys']), ','))
                           .withColumn("ak_keys", F.split(F.lit(','.join(dq_meta_dict['ak_keys'])), ','))
                           .withColumn('schema', F.lit(f"{dq_meta_dict['schema']}"))
                           .withColumn('from_table', F.lit(f"{dq_meta_dict['from_table']}"))
                           .withColumn('job_id', F.lit(dq_meta_dict['job_id']))
                           .withColumn('run_id', F.lit(dq_meta_dict['run_id']))
                           .withColumn('environment', F.lit(dq_meta_dict['environment']))
                           .select(*quarantine_cols)
                           )
    return detailed_quarantine


def create_df_header(df: p.DataFrame):
    """Create a data quality header based on output from create_dq_body().

    Parameters
    ----------
    df : : p.DataFrame
        PySpark Dataframe containing the data quality issues with a _quality_ak column.

    Returns
    -------
    p.DataFrame
        PySpark Dataframe containing the data quality issues summarised by schema, from_table, job_id,
        run_id, environment, _dt_ingest and _quality_ak columns.
    """
    # _check_required_fields(df.columns, [
    #                        'schema', 'from_table', 'job_id', 'run_id', 'environment', '_dt_ingest', '_quality_ak'])
    dq_header = (df
                 .groupBy('schema', 'from_table', 'job_id', 'run_id', 'environment', '_dt_ingest', '_quality_ak')
                 .agg(F.count('_quality_ak').alias('counted_ak_issues')))
    return dq_header
