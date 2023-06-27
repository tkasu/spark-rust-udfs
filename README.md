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

```shell
poetry run maturin develop --release
poetry run python -m python.spark_udfs.benchmark
```

Example output with Apple Macbook Air M1, 8GB:

```
native_sqrt_and_mol exec time: 0.3090, result: 21501849486.44
python_sqrt_and_mol exec time: 3.0891, result: 21501849486.56
python_sqrt_and_mol_arrow exec time: 1.5692, result: 21501849486.56
rust_sqrt_and_mol_udf exec time: 2.5222, result: 21501849486.56
rust_sqrt_and_mol_arrow_udf exec time: 0.9543, result: 21501849486.56
pandas_sqrt_and_mol exec time: 0.4755, result: 21501849486.56
```
