# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Data lake maintenance
# MAGIC
# MAGIC This notebook optimizes / vacuums delta tables to optimize space usage in Delta tables
# MAGIC (reclaimes spaces & reorganises files), improves query performance & reduces storage costs.
# MAGIC use them periodically.
# MAGIC

# COMMAND ----------

def optimize_delta_table(database_name, table_name):
    query = f"OPTIMIZE {database_name}.{table_name}"
    print(query)
    try:
        spark.sql(query)
    except Exception as e:
        print(e)
    return

# COMMAND ----------


def vacuum_delta_table(database_name, table_name):
    # vacuum files not required by versions older than the default retention period  (7d)
    query = f"VACUUM {database_name}.{table_name}"
    print(query)
    try:
        spark.sql(query)
    except Exception as e:
        print(e)
        return

# COMMAND ----------


def database_maintenance(database_name):
    # execute optimize & vacuum
    df = spark.sql("SHOW TABLES IN " + database_name)
    table_list = [str(t.tableName) for t in df.collect()]

    for table in table_list:
        optimize_delta_table(database_name, table)
        vacuum_delta_table(database_name, table)
    return


# COMMAND ----------
databases = ['db1', 'db2', 'db3']

for db in databases:
    database_maintenance(db)
