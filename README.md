# Rust UDF Spark Example

## Development

### Build

```shell
poetry run maturin develop
```

### Formatting

```shell
poetry run black .
```

### Test

```shell
poetry run pytest
```

## Run (unscientific) UDF benchmarks

NOTE! The order of the execution seems to matter atm, so the results are not reliable.

```shell
poetry run maturin develop --release
poetry run python -m python.spark_udfs.benchmark
```

Example output with Apple Macbook Air M1, 8GB:

```
--------------------------------------------------------------------------------
Benchmarking sqrt_and_mol -> sqrt(x) + 42
--------------------------------------------------------------------------------
native_sqrt_and_mol exec time: 0.2307, result: 21501849486.44
python_sqrt_and_mol exec time: 4.1304, result: 21501849486.56
python_sqrt_and_mol_arrow exec time: 1.5754, result: 21501849486.56
rust_sqrt_and_mol_udf exec time: 3.8009, result: 21501849486.56
rust_sqrt_and_mol_arrow_udf exec time: 0.9385, result: 21501849486.56
pandas_sqrt_and_mol exec time: 0.4944, result: 21501849486.56
--------------------------------------------------------------------------------
Benchmarking average_crt -> avg(clicks_arr / views_arr)
--------------------------------------------------------------------------------
Warmup: sum of clicks: 674895166, sum of views: 674895166
native_average_crt exec time: 1.5762, result: 0.1217
python_average_crt exec time: 2.5618, result: 0.1217
rust_average_crt_udf exec time: 1.7587, result: 0.1217
```
