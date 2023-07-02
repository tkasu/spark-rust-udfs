import pyspark.sql.functions as F
from pyspark.sql import Column


def native_sqrt_and_mol(value_col: Column) -> Column:
    return F.sqrt(value_col) + 42
