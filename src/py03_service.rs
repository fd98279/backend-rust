pub(crate) use log::{error, info};
use polars::io::json;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use pyo3::types::PyTuple;
use std::io;

#[pyclass]
#[derive(Debug, Default)]
pub(crate) struct PyMessage {
    #[pyo3(get, set)]
    message_id: String,
    #[pyo3(get, set)]
    key: String,
    #[pyo3(get, set)]
    sravz_ids: String,
    #[pyo3(get, set)]
    codes: String,
    #[pyo3(get, set)]
    df_parquet_file_path: String,
    #[pyo3(get, set)]
    output: String,
    #[pyo3(get, set)]
    json_keys: String,
    #[pyo3(get, set)]
    llm_query: String,
}

#[pymethods]
impl PyMessage {
    #[new]
    pub(crate) fn new(
        message_id: String, //Identify message in Python
        key: String,        // Used to save output file
        sravz_ids: String,
        codes: String,
        df_parquet_file_path: String,
        json_keys: String,
        llm_query: String,
    ) -> Self {
        PyMessage {
            message_id,
            key,
            sravz_ids,
            codes,
            df_parquet_file_path,
            output: String::new(),
            json_keys: json_keys,
            llm_query: llm_query,
        }
    }
}

pub(crate) fn run_py_module(py_message: PyMessage) -> Result<(), Box<std::io::Error>> {
    Python::with_gil(|py| {
        let activators = PyModule::from_code(
            py,
            r#"
import sys
import os
sys.path.append("/workspace/backend-rust/src/sravz_rust_py")        
# Used in the prod docker container
sys.path.append("/app/src/")     
sys.path.append("/app/src/sravz_rust_py")     
os.environ['MPLCONFIGDIR'] = "/tmp/matplotlib/"
def run(py_message, slope=0.01):
    try:
        from main import run
        return run(py_message)    
    except Exception as e:  # pylint: disable=broad-except
        print("Error occurred: %s", e)
        raise e
    "#,
            "activators.py",
            "activators",
        );

        match activators {
            Ok(_activator) => {
                let result = _activator.getattr("run"); // ?.call(args, Some(kwargs));

                match result {
                    Ok(py_result) => {
                        // Try to extract the result as a String
                        // let args = Py::new(
                        //     py,
                        //     PyMessage::new(message_key.to_string(), "abc123".to_string()),
                        // );
                        // let args = PyTuple::new(py, &[message_key]);
                        let args = PyTuple::new(
                            py,
                            &[
                                // PyMessage::new(message_key.to_string(),
                                // "fund_us_fbgrx.json,fund_us_fsptx.json,fund_us_fgrcx.json,fund_us_ekoax.json,fund_us_fzalx.json".to_string())
                                //     .into_py(py),
                                py_message.into_py(py),
                            ],
                        );
                        let kwargs = [("slope", 0.2)].into_py_dict(py);
                        let rust_result = py_result.call(args, Some(kwargs));
                        match rust_result {
                            Ok(result) => {
                                info!("Python Result as String: {}", result);
                            }
                            Err(err) => {
                                // Handle the Python error
                                error!("Python Error: {}", err);

                                // Access the traceback if available
                                if let Some(traceback) = PyErr::fetch(py).traceback(py) {
                                    // Handle the traceback as needed
                                    error!("Traceback: {:?}", traceback);
                                    return Err(Box::new(io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("Service error: {:?}", err),
                                    )));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        // Handle the Python error
                        error!("Python Error: {}", err);
                        // Access the traceback if available
                        if let Some(traceback) = PyErr::fetch(py).traceback(py) {
                            // Handle the traceback as needed
                            error!("Traceback: {:?}", traceback);
                            return Err(Box::new(io::Error::new(
                                io::ErrorKind::Other,
                                format!("Service error: {:?}", err),
                            )));
                        }
                    }
                }
            }
            Err(err) => {
                // Handle the Python error
                error!("Python Error: {}", err);
                // Access the traceback if available
                if let Some(traceback) = PyErr::fetch(py).traceback(py) {
                    // Handle the traceback as needed
                    error!("Traceback: {:?}", traceback);
                    return Err(Box::new(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Service error: {:?}", err),
                    )));
                }
            }
        }
        Ok(())
    })
}
