import pandas as pd
import polars as pl
import pyarrow as pa
from pyspark.sql.functions import pandas_udf


@pandas_udf("float")
def polars_udf_sqrt_and_mol(value_col: pd.Series) -> pd.Series:
    pl_arr = pl.from_pandas(value_col)
    res_arr = pl_arr.sqrt() + 42.0
    return res_arr.to_pandas()


@pandas_udf("double")
def polars_udf_sqrt_and_mol_arrow_optimized(value_col: pd.Series) -> pd.Series:
    pl_arr = pl.from_arrow(pa.Array.from_pandas(value_col), rechunk=False)
    res_arr = pl_arr.sqrt() + 42.0
    return res_arr.to_pandas(use_pyarrow_extension_array=True, zero_copy_only=True)
