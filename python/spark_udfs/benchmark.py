from timeit import default_timer as timer

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column, SparkSession
from pyspark.sql.pandas.functions import pandas_udf


def benchmark():
    spark = _get_spark_session()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    from python.spark_udfs.rust import (
        rust_sqrt_and_mol_udf,
        rust_sqrt_and_mol_arrow_udf,
    )

    df = spark.range(10_000_000).select(F.col("id").alias("value")).repartition(8)
    df.persist()
    # trigger persist
    df_count = df.count()

    pandas_udf_sqrt_and_mol = pandas_udf(pandas_sqrt_and_mol, "float")

    _time_and_log_spark_fn_exec_and_sum(df, native_sqrt_and_mol)
    _time_and_log_spark_fn_exec_and_sum(df, rust_sqrt_and_mol_udf)
    _time_and_log_spark_fn_exec_and_sum(df, rust_sqrt_and_mol_arrow_udf)
    _time_and_log_spark_fn_exec_and_sum(df, pandas_udf_sqrt_and_mol)

    df.unpersist()


def native_sqrt_and_mol(value_col: Column) -> Column:
    return F.sqrt(value_col) + 42


def pandas_sqrt_and_mol(value_col: pd.Series) -> pd.Series:
    return value_col.pow(1 / 2) + 42


def _time_and_log_spark_fn_exec_and_sum(df, fn):
    start = timer()
    res = df.withColumn("res", fn("value")).select(F.sum("res")).collect()[0][0]
    end = timer()
    print(f"{fn.__name__} exec time: {end - start:.4f}, result: {res:.2f}")


def _get_spark_session():
    return SparkSession.builder.appName("pytest-pyspark-local-testing").getOrCreate()


if __name__ == "__main__":
    benchmark()
