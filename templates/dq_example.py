# Databricks notebook source

from amif.dq_helper import create_dq_body, create_df_header
from amif.utils import append_delta
# replace with sys.append & corresponding module if running in on databricks

# widget input
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("job_id", "test12345")
dbutils.widgets.text("run_id", "test12345")

# get job & run id as widgets , indicate by double curly brackets https://stackoverflow.com/a/75107910
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# STATIC variables
table_name = 'afis_glaccounthierarchynode'
ak_keys = [
    "GLAccountHierarchy",
    "HierarchyNode",
    "ValidityStartDate"
]
catalogue = 'bronze_stg'

# create meta_dict
dq_meta_dict = {
    'ak_keys': ak_keys,
    'schema': catalogue,
    'from_table': table_name,
    'job_id': job_id,
    'run_id': run_id,
    'environment': environment
}

# COMMAND ----------

# load initial df
df = spark.sql(f'SELECT * FROM dpmf_dev.{dq_meta_dict['schema']}.{dq_meta_dict['table_name']}_cdc')
df_filtered = df.where((F.col("_quality_ak") == ''))
print(df_filtered.count())

df_quarantine = df.exceptAll(df_filtered)
print(df_quarantine.count())

# COMMAND ----------

detailed_quarantine = create_dq_body(df_quarantine, dq_meta_dict)
append_delta(df, 'hive_metastore.default.dq_body')

dq_header = create_df_header(detailed_quarantine)
append_delta(dq_header, 'hive_metastore.default.dq_header')
