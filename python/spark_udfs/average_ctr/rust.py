from typing import List

from pyspark.sql.functions import udf

from spark_rust_udfs import average_crt


@udf("float")
def rust_udf_average_crt_udf(clicks: List[int], views: List[int]) -> float:
    return average_crt(clicks, views)
