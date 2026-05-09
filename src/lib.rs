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
mod exfat;
mod fat32;
mod ext4;
mod gpt;
mod events;
mod initramfs_patcher;
mod kmod;
mod extraction;
mod grub_patcher;
mod squashfs;
mod iso9660;


use drives::{list_removable_drives, DriveInfo};
use image_parser::{inspect_image, ImageStats, BootCapabilities, WindowsMetadata};
use test_utils::{
    create_mock_iso, FakeDrive, test_verify_fake_drive_sync, create_mock_esd, hash_sha256
};
use events::{EventMsg, ProgressStream, AbortToken};
use crate::extraction::{extract_image, get_wim_info_from_iso};
use writer::{
    write_image_dd, write_image_iso, format_usb_drive, inspect_usb_partition};
use verify::{destructive_verify_usb_size};
use initramfs_patcher::{patch_initramfs_py};
use grub_patcher::scan_efi_pattern_py;
use squashfs::{extract_file_from_squashfs_py};


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

    m.add_function(wrap_pyfunction!(scan_efi_pattern_py, m)?)?; 
    // m.add_function(wrap_pyfunction!(drives::_list_all_volumes, m)?)?; 
    
    // Writing
    m.add_class::<AbortToken>()?;
    m.add_class::<EventMsg>()?;
    m.add_class::<ProgressStream>()?;
    m.add_function(wrap_pyfunction!(get_wim_info_from_iso, m)?)?; 
    m.add_function(wrap_pyfunction!(inspect_usb_partition, m)?)?; 
    m.add_function(wrap_pyfunction!(extract_image, m)?)?; 
    m.add_function(wrap_pyfunction!(format_usb_drive, m)?)?; 
    m.add_function(wrap_pyfunction!(write_image_dd, m)?)?; 
    m.add_function(wrap_pyfunction!(write_image_iso, m)?)?; 

    // Testing
    m.add_class::<FakeDrive>()?;
    m.add_function(wrap_pyfunction!(create_mock_iso, m)?)?; 
    m.add_function(wrap_pyfunction!(test_verify_fake_drive_sync, m)?)?; 
    m.add_function(wrap_pyfunction!(create_mock_esd, m)?)?; 
    m.add_function(wrap_pyfunction!(hash_sha256, m)?)?; 

    // Signing
    m.add_function(wrap_pyfunction!(sbsign::sign_efi_binary, m)?)?;
    m.add_function(wrap_pyfunction!(sbsign::generate_secure_boot_keys, m)?)?;
    
    // Patching
    m.add_function(wrap_pyfunction!(patch_initramfs_py, m)?)?;
    m.add_function(wrap_pyfunction!(extract_file_from_squashfs_py, m)?)?;

    // Windows .esd file parsing
    m.add_class::<esd::WimFileEntry>()?;
    m.add_class::<esd::EsdArchive>()?;

    Ok(())
}