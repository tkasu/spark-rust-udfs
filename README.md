# Rust UDF Spark Example

## Development

### Requirements

* rustup
* poetry
* jdk 11
* sbt

### Build

#### Python & Rust

```shell
poetry install
poetry run maturin develop
```

#### Scala

```shell
bash -c "cd scala/scala-udfs && sbt package"
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

NOTE! The order of the execution seems to matter a bit, so the results are not totally reliable.

```shell
poetry run maturin develop --release
poetry run python -m python.spark_udfs.benchmark
```

Example output with Apple Macbook Air M1, 8GB:

```
--------------------------------------------------------------------------------
Benchmarking sqrt_and_mol -> sqrt(x) + 42
--------------------------------------------------------------------------------
python_sqrt_and_mol exec time: 20.2981, result: 237802256859.68
python_sqrt_and_mol_arrow exec time: 8.8571, result: 237802256859.68
rust_sqrt_and_mol_udf exec time: 17.9998, result: 237802256859.68
rust_sqrt_and_mol_arrow_udf exec time: 5.5307, result: 237802256859.68
pandas_sqrt_and_mol exec time: 3.4952, result: 237802256859.68
scala_sqrt_and_mol_fn exec time: 1.1056, result: 237802256859.78
native_sqrt_and_mol exec time: 0.4935, result: 237802256859.78
--------------------------------------------------------------------------------
Benchmarking average_crt -> avg(clicks_arr / views_arr)
--------------------------------------------------------------------------------
Warmup: sum of clicks: 674907096, sum of views: 674907096
native_average_crt exec time: 4.3353, result: 0.1216
python_average_crt exec time: 3.7523, result: 0.1216
rust_average_crt_udf exec time: 2.5552, result: 0.1216
```
