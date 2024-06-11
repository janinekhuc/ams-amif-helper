import pytest
from amif.dq_helper import create_df_header, create_dq_body
from pyspark.sql import SparkSession


def test_create_df_header():
    # Arrange
    spark = SparkSession.builder.getOrCreate()
    data = [
        ("schema1", "table1", "job1", "run1", "env1", "2022-01-01", "issue1"),
        ("schema1", "table1", "job1", "run1", "env1", "2022-01-01", "issue2"),
        ("schema1", "table1", "job1", "run1", "env1", "2022-01-01", "issue1"),
        ("schema2", "table1", "job1", "run1", "env1", "2022-01-01", "issue1"),
    ]
    columns = ["schema", "from_table", "job_id", "run_id",
               "environment", "_dt_ingest", "_quality_ak"]
    df = spark.createDataFrame(data, columns)

    expected_result = [
        ("schema1", "table1", "job1", "run1", "env1", "2022-01-01", "issue1", 2),
        ("schema1", "table1", "job1", "run1", "env1", "2022-01-01", "issue2", 1),
        ("schema2", "table1", "job1", "run1", "env1", "2022-01-01", "issue1", 1),
    ]
    expected_columns = ["schema", "from_table", "job_id", "run_id",
                        "environment", "_dt_ingest", "_quality_ak", "counted_ak_issues"]
    expected_df = spark.createDataFrame(expected_result, expected_columns)

    # Act
    result_df = create_df_header(df)

    # Assert
    assert result_df.collect() == expected_df.collect(
    ), "Creating data quality header failed"
