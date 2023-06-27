import math
from timeit import default_timer as timer
from typing import List

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column, SparkSession


def benchmark():
    spark = _get_spark_session()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    from python.spark_udfs.rust import (
        rust_sqrt_and_mol_udf,
        rust_sqrt_and_mol_arrow_udf,
        rust_average_crt_udf,
    )

    print("-" * 80)
    print("Benchmarking sqrt_and_mol -> sqrt(x) + 42")
    print("-" * 80)
    df_simple = (
        spark.range(10_000_000).select(F.col("id").alias("value")).repartition(8)
    )
    df_simple.persist()
    # trigger persist
    _ = df_simple.count()

    python_udf_sqrt_and_mol = F.udf(python_sqrt_and_mol, "float")
    python_udf_sqrt_and_mol_arrow = F.pandas_udf(python_sqrt_and_mol_arrow, "float")
    pandas_udf_sqrt_and_mol = F.pandas_udf(pandas_sqrt_and_mol, "float")

    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, native_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, python_udf_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, python_udf_sqrt_and_mol_arrow)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, rust_sqrt_and_mol_udf)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, rust_sqrt_and_mol_arrow_udf)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, pandas_udf_sqrt_and_mol)

    df_simple.unpersist()

    print("-" * 80)
    print("Benchmarking average_crt -> avg(clicks_arr / views_arr)")
    print("-" * 80)

    pdf_clicks_views = pd.DataFrame(
        {
            "clicks": (
                np.random.randint(low=0, high=10, size=5).tolist()
                for _ in range(2_500_000)
            ),
            "views": (
                np.random.randint(low=9, high=100, size=5).tolist()
                for _ in range(2_500_000)
            ),
        }
    )
    df_clicks_views = spark.createDataFrame(pdf_clicks_views).repartition(32)

    df_clicks_views.persist()
    # trigger persist
    _ = df_clicks_views.count()

    clicks = df_clicks_views.select(
        F.sum(
            F.aggregate(
                F.col("views"), F.lit(0).cast("bigint"), lambda acc, nxt: acc + nxt
            )
        )
    ).collect()[0][0]
    views = df_clicks_views.select(
        F.sum(
            F.aggregate(
                F.col("views"), F.lit(0).cast("bigint"), lambda acc, nxt: acc + nxt
            )
        )
    ).collect()[0][0]

    print(f"Warmup: sum of clicks: {clicks}, sum of views: {views}")

    python_average_crt_udf = F.udf(python_average_crt, "float")

    _time_and_log_average_crt_fn_exec_and_sum(df_clicks_views, native_average_crt)
    _time_and_log_average_crt_fn_exec_and_sum(df_clicks_views, python_average_crt_udf)
    _time_and_log_average_crt_fn_exec_and_sum(df_clicks_views, rust_average_crt_udf)

    df_clicks_views.unpersist()


def native_sqrt_and_mol(value_col: Column) -> Column:
    return F.sqrt(value_col) + 42


def pandas_sqrt_and_mol(value_col: pd.Series) -> pd.Series:
    return value_col.pow(1 / 2) + 42


def python_sqrt_and_mol(value: int) -> float:
    return math.sqrt(value) + 42


def python_sqrt_and_mol_arrow(s: pd.Series) -> pd.Series:
    return s.apply(python_sqrt_and_mol)


def python_average_crt(clicks: List[int], views: List[int]) -> float:
    return sum(click / view for click, view in zip(clicks, views)) / len(clicks)


def native_average_crt(clicks: Column, views: Column) -> Column:
    ctrs = F.transform(
        F.arrays_zip(clicks, views), lambda struct: struct.clicks / struct.views
    )
    return F.aggregate(ctrs, F.lit(0.0), lambda acc, crt: acc + crt) / F.size(clicks)


def _time_and_log_sqrt_and_mol_fn_exec_and_sum(df, fn):
    start = timer()
    res = df.withColumn("res", fn("value")).select(F.sum("res")).collect()[0][0]
    end = timer()
    print(f"{fn.__name__} exec time: {end - start:.4f}, result: {res:.2f}")


def _time_and_log_average_crt_fn_exec_and_sum(df, fn):
    start = timer()
    res = (
        df.withColumn("res", fn("clicks", "views")).select(F.avg("res")).collect()[0][0]
    )
    end = timer()
    print(f"{fn.__name__} exec time: {end - start:.4f}, result: {res:.4f}")


def _get_spark_session():
    return SparkSession.builder.appName("pytest-pyspark-local-testing").getOrCreate()


if __name__ == "__main__":
    benchmark()
