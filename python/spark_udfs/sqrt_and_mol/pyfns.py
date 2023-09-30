import math

import pandas as pd
from pyspark.sql.functions import pandas_udf, udf


@udf("float")
def python_udf_sqrt_and_mol(value: int) -> float:
    return math.sqrt(value) + 42


@udf("float", useArrow=True)
def python_udf_spark35arrow_sqrt_and_mol(value: int) -> float:
    return math.sqrt(value) + 42


@pandas_udf("float")
def python_arrow_udf_sqrt_and_mol(s: pd.Series) -> pd.Series:
    return s.apply(lambda x: math.sqrt(x) + 42)
