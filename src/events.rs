use std::{
    sync::{Arc, mpsc, Mutex, atomic::{AtomicBool, Ordering}},
};

use pyo3::{prelude::*, };



// -- Communication constructs between Python and Rust

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct AbortToken {
    pub(crate) flag: Arc<AtomicBool>,
}

#[pymethods]
impl AbortToken {
    #[new]
    pub fn new() -> Self {
        Self { flag: Arc::new(AtomicBool::new(false)) }
    }
    pub fn abort(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }
}

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct EventMsg {
    #[pyo3(get)]
    pub msg_type: String,
    #[pyo3(get)]
    pub written: u64,
    #[pyo3(get)]
    pub total: u64,
    #[pyo3(get)]
    pub text: String,
}

impl EventMsg {
    pub fn progress(written: u64, total: u64) -> Self {
        Self { msg_type: "PROGRESS".to_string(), written, total, text: String::new() }
    }
    pub fn phase(text: &str) -> Self {
        Self { msg_type: "PHASE".to_string(), written: 0, total: 0, text: text.to_string() }
    }
    pub fn log(text: &str) -> Self {
        Self { msg_type: "LOG".to_string(), written: 0, total: 0, text: text.to_string() }
    }
    pub fn done(text: &str) -> Self {
        Self { msg_type: "DONE".to_string(), written: 0, total: 0, text: text.to_string() }
    }
    pub fn error(text: &str) -> Self {
        Self { msg_type: "ERROR".to_string(), written: 0, total: 0, text: text.to_string() }
    }
}

#[pyclass]
pub struct ProgressStream {
    pub(crate) rx: Mutex<mpsc::Receiver<EventMsg>>,
}

#[pymethods]
impl ProgressStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }

    fn __next__(&self, py: Python) -> PyResult<Option<EventMsg>> {
        match py.detach(|| self.rx.lock().unwrap().recv()) {
            Ok(msg) => Ok(Some(msg)),
            Err(_) => Ok(None), 
        }
    }
}
