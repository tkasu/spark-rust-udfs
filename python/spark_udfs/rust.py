from pyspark.sql.functions import udf

from spark_rust_udfs import sqrt_and_mol


@udf("float")
def rust_sqrt_and_mol(value: int) -> float:
    return sqrt_and_mol(value)
