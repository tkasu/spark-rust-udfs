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
poetry run python -m python.spark_udfs.benchmark
```

Example output with Apple Macbook Air M1, 8GB:

```
native_sqrt_and_mol exec time: 0.2803, result: 21501849486.44
rust_sqrt_and_mol_udf exec time: 4.6302, result: 21501849486.56
rust_sqrt_and_mol_arrow_udf exec time: 2.4779, result: 21501849486.56
pandas_sqrt_and_mol exec time: 0.5406, result: 21501849486.56
```
