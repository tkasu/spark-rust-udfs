import pyspark.sql.functions as F
from pyspark.sql import Column


def native_average_crt(clicks: Column, views: Column) -> Column:
    ctrs = F.transform(
        F.arrays_zip(clicks, views), lambda struct: struct.clicks / struct.views
    )
    return F.aggregate(ctrs, F.lit(0.0), lambda acc, crt: acc + crt) / F.size(clicks)
