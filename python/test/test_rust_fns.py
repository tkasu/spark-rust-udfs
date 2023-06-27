from spark_rust_udfs import average_crt, sqrt_and_mol


def test_average_crt_rustfn():
    assert average_crt([1, 2, 8], [5, 4, 10]) == 0.5


def test_sqrt_and_mol_rustfn():
    assert sqrt_and_mol(4) == 44.0
