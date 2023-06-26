from spark_rust_udfs import sqrt_and_mol


def test_sqrt_and_mol_rustfn():
    assert sqrt_and_mol(4) == 44.0
