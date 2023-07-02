from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Column
from pyspark.sql.types import DoubleType


def get_scala_udf_sqrt_and_mol_fn(spark: SparkSession) -> Callable[[str], Column]:
    spark.udf.registerJavaFunction(
        "scala_sqrt_and_mol", "com.github.tkasu.udfs.SqrtAndMolUDF", DoubleType()
    )

    def scala_udf_sqrt_and_mol(col_name: str) -> Column:
        return F.expr(f"scala_sqrt_and_mol({col_name})")

    return scala_udf_sqrt_and_mol
