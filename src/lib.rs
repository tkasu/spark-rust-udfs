use pyo3::prelude::*;

#[pyfunction]
pub fn sqrt_and_mol(value: usize) -> f64 {
    (value as f64).sqrt() + 42.0
}

#[pymodule]
fn spark_rust_udfs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sqrt_and_mol, m)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqrt_and_and_mol() {
        assert_eq!(sqrt_and_mol(4), 44.0);
    }
}
