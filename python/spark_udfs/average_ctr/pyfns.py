from typing import List

from pyspark.sql.functions import udf


@udf("float")
def python_udf_average_crt(clicks: List[int], views: List[int]) -> float:
    return sum(click / view for click, view in zip(clicks, views)) / len(clicks)
