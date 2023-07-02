import pandas as pd
import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("pytest-pyspark-local-testing").getOrCreate()


def test_rust_sqrt_and_mol_udf(spark: SparkSession):
    from python.spark_udfs.sqrt_and_mol.rust import rust_sqrt_and_mol_udf

    pdf = pd.DataFrame({"value": [1, 4], "expected_res": [43.0, 44.0]})
    df = spark.createDataFrame(pdf)
    res_df = df.withColumn("res", rust_sqrt_and_mol_udf("value"))
    assert res_df.filter(F.col("res") != F.col("expected_res")).isEmpty()


def test_rust_sqrt_and_mol_arrow_udf(spark: SparkSession):
    from python.spark_udfs.sqrt_and_mol.rust import rust_sqrt_and_mol_arrow_udf

    pdf = pd.DataFrame({"value": [1, 4], "expected_res": [43.0, 44.0]})
    df = spark.createDataFrame(pdf)
    res_df = df.withColumn("res", rust_sqrt_and_mol_arrow_udf("value"))
    assert res_df.filter(F.col("res") != F.col("expected_res")).isEmpty()
