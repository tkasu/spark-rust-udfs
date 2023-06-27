use pyo3::prelude::*;

#[pyfunction]
pub fn sqrt_and_mol(value: usize) -> f64 {
    (value as f64).sqrt() + 42.0
}

#[pyfunction]
pub fn average_crt(clicks: Vec<usize>, views: Vec<usize>) -> f64 {
    if clicks.len() != views.len() {
        panic!("clicks and views must have the same length");
    }
    if clicks.len() == 0 {
        return 0.0;
    }
    clicks.iter().zip(views.iter())
        .map(|(click_count, view_count)| *click_count as f64 / *view_count as f64)
        .sum::<f64>() / clicks.len() as f64
}

#[pymodule]
fn spark_rust_udfs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sqrt_and_mol, m)?)?;
    m.add_function(wrap_pyfunction!(average_crt, m)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqrt_and_and_mol() {
        assert_eq!(sqrt_and_mol(4), 44.0);
    }

    #[test]
    fn test_average_crt() {
        assert_eq!(average_crt(vec![1, 2, 8], vec![5, 4, 10]), 0.5);
    }
}
