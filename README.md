# Spark UDFs examples

This project started as a trial of creating Maturin-based Rust UDFs for Apache Spark.
However, it turned out to be my Spark UDF testing playground.

Note that the benchmarks are run within single machine, and the order of the execution impact the results
slightly. So the results are not completely deterministic, but they give some idea of the performance of the UDFs.

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

```shell
poetry run maturin develop --release
poetry run python -m python.spark_udfs.benchmark
```

Example output with Apple Macbook Air M1, 8GB:

```
--------------------------------------------------------------------------------
Benchmarking sqrt_and_mol -> sqrt(x) + 42
--------------------------------------------------------------------------------
native_sqrt_and_mol exec time: 0.3383, result: 237802256859.78
python_udf_sqrt_and_mol exec time: 12.7751, result: 237802256859.68
python_arrow_udf_sqrt_and_mol exec time: 4.1689, result: 237802256859.68
pandas_udf_sqrt_and_mol exec time: 2.1664, result: 237802256859.68
rust_sqrt_and_mol_udf exec time: 13.3613, result: 237802256859.68
rust_sqrt_and_mol_arrow_udf exec time: 3.3647, result: 237802256859.68
polars_udf_sqrt_and_mol exec time: 2.1380, result: 237802256859.68
polars_udf_sqrt_and_mol_arrow_optimized exec time: 2.3455, result: 237802256859.78
scala_udf_sqrt_and_mol exec time: 0.4981, result: 237802256859.78
--------------------------------------------------------------------------------
Benchmarking average_crt -> avg(clicks_arr / views_arr)
--------------------------------------------------------------------------------
Warmup: sum of clicks: 674870720, sum of views: 674870720
native_average_crt exec time: 1.6115, result: 0.1216
python_udf_average_crt exec time: 2.4822, result: 0.1216
rust_udf_average_crt_udf exec time: 1.6440, result: 0.1216
```
