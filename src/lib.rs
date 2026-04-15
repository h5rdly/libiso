use pyo3::prelude::*;

mod drives;
mod image_parser;
mod test_utils;
mod writer;
mod signature;
mod io;
mod dbx;

use drives::{list_removable_drives, DriveInfo};
use image_parser::{inspect_image, ImageStats, BootCapabilities, WindowsMetadata};
use test_utils::{create_mock_iso};
use writer::{write_image_dd, write_image_iso};


#[pymodule]
fn _libiso(m: &Bound<'_, PyModule>) -> PyResult<()> {

    m.add_class::<DriveInfo>()?;
    m.add_function(wrap_pyfunction!(list_removable_drives, m)?)?;
    
    m.add_class::<BootCapabilities>()?;
    m.add_class::<WindowsMetadata>()?;
    m.add_class::<ImageStats>()?;
    m.add_function(wrap_pyfunction!(inspect_image, m)?)?;
    
    m.add_function(wrap_pyfunction!(write_image_dd, m)?)?; 
    m.add_function(wrap_pyfunction!(write_image_iso, m)?)?; 

    m.add_function(wrap_pyfunction!(create_mock_iso, m)?)?; 
    Ok(())
}