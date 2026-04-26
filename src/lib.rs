use pyo3::prelude::*;

mod drives;
mod image_parser;
mod udf;
mod test_utils;
mod writer;
mod signature;
mod io;
mod dbx;
mod verify;
mod esd;
mod lzms;
mod lzms_arrays;
mod bootloader;
mod sbsign;


use drives::{list_removable_drives, DriveInfo};
use image_parser::{inspect_image, ImageStats, BootCapabilities, WindowsMetadata};
use test_utils::{create_mock_iso, FakeDrive, test_verify_fake_drive_sync, create_mock_esd};
use writer::{write_image_dd, write_image_iso, format_usb_drive, inspect_usb_partition, 
    AbortToken, EventMsg, ProgressStream};
use verify::{destructive_verify_usb_size};


#[pymodule]
fn _libiso(m: &Bound<'_, PyModule>) -> PyResult<()> {

    // Inspection
    m.add_class::<DriveInfo>()?;
    m.add_class::<BootCapabilities>()?;
    m.add_class::<WindowsMetadata>()?;
    m.add_class::<ImageStats>()?;
    m.add_function(wrap_pyfunction!(list_removable_drives, m)?)?;
    m.add_function(wrap_pyfunction!(inspect_image, m)?)?;
    m.add_function(wrap_pyfunction!(destructive_verify_usb_size, m)?)?; 
    // m.add_function(wrap_pyfunction!(drives::_list_all_volumes, m)?)?; 
    
    // Writing
    m.add_class::<AbortToken>()?;
    m.add_class::<EventMsg>()?;
    m.add_class::<ProgressStream>()?;
    m.add_function(wrap_pyfunction!(inspect_usb_partition, m)?)?; 
    m.add_function(wrap_pyfunction!(format_usb_drive, m)?)?; 
    m.add_function(wrap_pyfunction!(write_image_dd, m)?)?; 
    m.add_function(wrap_pyfunction!(write_image_iso, m)?)?; 

    // Testing
    m.add_class::<FakeDrive>()?;
    m.add_function(wrap_pyfunction!(create_mock_iso, m)?)?; 
    m.add_function(wrap_pyfunction!(test_verify_fake_drive_sync, m)?)?; 
    m.add_function(wrap_pyfunction!(create_mock_esd, m)?)?; 

    // Signing
    m.add_function(wrap_pyfunction!(sbsign::sign_efi_binary, m)?)?;
    m.add_function(wrap_pyfunction!(sbsign::generate_secure_boot_keys, m)?)?;
    
    // Windows .esd file parsing
    m.add_class::<esd::WimFileEntry>()?;
    m.add_class::<esd::EsdArchive>()?;

    Ok(())
}