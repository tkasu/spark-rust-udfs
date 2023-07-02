import pandas as pd
from pyspark.sql.functions import pandas_udf


@pandas_udf("float")
def pandas_udf_sqrt_and_mol(value_col: pd.Series) -> pd.Series:
    return value_col.pow(1 / 2) + 42
