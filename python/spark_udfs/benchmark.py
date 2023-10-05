import os
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def benchmark():
    spark = _get_spark_session()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

    sqrt_and_mol_benchmark(spark)
    average_ctr_benchmark(spark)


def sqrt_and_mol_benchmark(spark: SparkSession):
    from python.spark_udfs.sqrt_and_mol.pyfns import (
        python_udf_sqrt_and_mol,
        python_udf_spark35arrow_sqrt_and_mol,
        python_arrow_udf_sqrt_and_mol,
    )
    from python.spark_udfs.sqrt_and_mol.rust import (
        rust_sqrt_and_mol_udf,
        rust_sqrt_and_mol_arrow_udf,
    )
    from python.spark_udfs.sqrt_and_mol.pandas_fns import pandas_udf_sqrt_and_mol
    from python.spark_udfs.sqrt_and_mol.polars_fns import (
        polars_udf_sqrt_and_mol,
        polars_udf_sqrt_and_mol_arrow_optimized,
    )
    from python.spark_udfs.sqrt_and_mol.scala import get_scala_udf_sqrt_and_mol_fn
    from python.spark_udfs.sqrt_and_mol.native import native_sqrt_and_mol

    cpu_count = os.cpu_count()

    print("-" * 80)
    print("Benchmarking sqrt_and_mol -> sqrt(x) + 42")
    print("-" * 80)
    df_simple = (
        spark.range(50_000_000)
        .select(F.col("id").alias("value"))
        .repartition(cpu_count)
    )
    df_simple.persist()
    # trigger persist
    _ = df_simple.count()
    scala_udf_sqrt_and_mol_fn = get_scala_udf_sqrt_and_mol_fn(spark)

    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, native_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, python_udf_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(
        df_simple, python_udf_spark35arrow_sqrt_and_mol
    )
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, python_arrow_udf_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, pandas_udf_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, rust_sqrt_and_mol_udf)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, rust_sqrt_and_mol_arrow_udf)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, polars_udf_sqrt_and_mol)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(
        df_simple, polars_udf_sqrt_and_mol_arrow_optimized
    )
    _time_and_log_sqrt_and_mol_map_in_pandas_exec_and_sum(df_simple)
    _time_and_log_sqrt_and_mol_map_in_arrow_polars_exec_and_sum(df_simple)
    _time_and_log_sqrt_and_mol_fn_exec_and_sum(df_simple, scala_udf_sqrt_and_mol_fn)

    df_simple.unpersist()


def average_ctr_benchmark(spark: SparkSession):
    print("-" * 80)
    print("Benchmarking average_crt -> avg(clicks_arr / views_arr)")
    print("-" * 80)

    from python.spark_udfs.average_ctr.rust import rust_udf_average_crt_udf
    from python.spark_udfs.average_ctr.pyfns import (
        python_udf_average_crt,
        python_udf_spark35arrow_average_crt,
    )
    from python.spark_udfs.average_ctr.native import native_average_crt

    cpu_count = os.cpu_count()

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
    df_clicks_views = spark.createDataFrame(pdf_clicks_views).repartition(cpu_count * 4)

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

    _time_and_log_average_crt_fn_exec_and_sum(df_clicks_views, native_average_crt)
    _time_and_log_average_crt_fn_exec_and_sum(df_clicks_views, python_udf_average_crt)
    _time_and_log_average_crt_fn_exec_and_sum(
        df_clicks_views, python_udf_spark35arrow_average_crt
    )
    _time_and_log_average_crt_fn_exec_and_sum(df_clicks_views, rust_udf_average_crt_udf)

    df_clicks_views.unpersist()


def _time_and_log_sqrt_and_mol_fn_exec_and_sum(df, fn):
    start = timer()
    res = df.withColumn("res", fn("value")).select(F.sum("res")).collect()[0][0]
    end = timer()
    print(f"{fn.__name__} exec time: {end - start:.4f}, result: {res:.2f}")


def _time_and_log_sqrt_and_mol_map_in_pandas_exec_and_sum(df):
    from typing import Iterator
    import pandas as pd

    # TODO: Move this function to pandas_fns.py
    #   Currently if doing so, it complains about `RuntimeError: SparkContext or SparkSession should be created first.`
    def map_in_pandas_fn(
        iterator: Iterator[pd.DataFrame],
    ) -> Iterator[pd.DataFrame]:
        for pdf in iterator:
            pdf["res"] = pdf["value"].pow(1 / 2) + 42
            yield pdf

    start = timer()
    res = (
        df.mapInPandas(map_in_pandas_fn, "value long, res double")
        .select(F.sum("res"))
        .collect()[0][0]
    )
    end = timer()
    print(
        f"{map_in_pandas_fn.__name__}, exec time: {end - start:.4f}, result: {res:.2f}"
    )


def _time_and_log_sqrt_and_mol_map_in_arrow_polars_exec_and_sum(df):
    from typing import Iterator
    import pyarrow as pa
    import polars as pl

    # TODO: Move this function to polars_fns.py
    #   Currently if doing so, it complains about `RuntimeError: SparkContext or SparkSession should be created first.`
    def map_in_arrow_polars(
        iterator: Iterator[pa.RecordBatch],
    ) -> Iterator[pa.RecordBatch]:
        for batch in iterator:
            df_pl = pl.from_arrow(batch, rechunk=False).with_columns(
                [(pl.col("value").sqrt() + 42.0).alias("res")]
            )

            arrow_batches = df_pl.to_arrow().to_batches()
            if len(arrow_batches) != 1:
                raise RuntimeError("Expected only one batch")
            arrow_batch = arrow_batches[0]
            yield arrow_batch

    start = timer()
    res = (
        df.mapInArrow(map_in_arrow_polars, "value long, res double")
        .select(F.sum("res"))
        .collect()[0][0]
    )
    end = timer()
    print(
        f"{map_in_arrow_polars.__name__}, exec time: {end - start:.4f}, result: {res:.2f}"
    )


def _time_and_log_average_crt_fn_exec_and_sum(df, fn):
    start = timer()
    res = (
        df.withColumn("res", fn("clicks", "views")).select(F.avg("res")).collect()[0][0]
    )
    end = timer()
    print(f"{fn.__name__} exec time: {end - start:.4f}, result: {res:.4f}")


def _get_spark_session():
    return (
        SparkSession.builder.config(
            "spark.jars",
            "scala/scala-udfs/target/scala-2.13/scala-udfs_2.13-0.1.0-SNAPSHOT.jar",
        )
        .appName("pytest-pyspark-local-testing")
        .getOrCreate()
    )


if __name__ == "__main__":
    benchmark()
