use std::path::Path;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::DirectoryRef;

use pyo3::prelude::*;


#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct BootCapabilities {
    #[pyo3(get)]
    pub is_bootable: bool,
    #[pyo3(get)]
    pub supports_bios: bool,
    #[pyo3(get)]
    pub supports_uefi: bool,
    #[pyo3(get)]
    pub secure_boot_signed: bool,
}

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct WindowsMetadata {
    #[pyo3(get)]
    pub is_windows: bool,
    #[pyo3(get)]
    pub is_windows_11: bool,
    #[pyo3(get)]
    pub install_image_type: String, 
    #[pyo3(get)]
    pub supports_wintogo: bool,
}

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct ImageReport {
    #[pyo3(get)]
    pub file_path: String,
    #[pyo3(get)]
    pub size_bytes: u64,
    #[pyo3(get)]
    pub volume_label: String,
    #[pyo3(get)]
    pub is_isohybrid: bool,
    #[pyo3(get)]
    pub has_large_file: bool,
    
    #[pyo3(get)]
    pub boot_info: BootCapabilities,
    
    #[pyo3(get)]
    pub windows_info: Option<WindowsMetadata>, 
}


// Recursive Helper to Traverse the ISO Directory Tree
#[allow(clippy::too_many_arguments)]
fn scan_directory(
    iso: &IsoImage<File>, 
    dir_ref: DirectoryRef,
    has_large_file: &mut bool,
    supports_uefi: &mut bool,
    secure_boot_signed: &mut bool,
    is_windows: &mut bool,
    is_windows_11: &mut bool,
    install_image_type: &mut String,
) {
    let dir = iso.open_dir(dir_ref); 
    
    for entry_res in dir.entries() {
        let Ok(entry) = entry_res else { continue };
        
        // Skip current (.) and parent (..) directory markers
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" {
            continue;
        }

        // FIX: Extract the true name using Rock Ridge or Joliet decoding!
        let mut name = if entry.record.is_joliet_name() {
            entry.record.joliet_name() // Decodes Windows UTF-16
        } else {
            entry.display_name().into_owned() // Decodes Linux Rock Ridge or standard ASCII
        };

        // Remove the ISO9660 version suffix (e.g. ";1")
        if let Some(pos) = name.rfind(';') {
            name.truncate(pos);
        }

        if entry.is_directory() {
            if let Ok(sub_dir_ref) = entry.as_dir_ref(iso) {
                scan_directory(
                    iso, 
                    sub_dir_ref, 
                    has_large_file, 
                    supports_uefi, 
                    secure_boot_signed,
                    is_windows,
                    is_windows_11,
                    install_image_type
                );
            }
        } else {
            if entry.total_size() >= 4_000_000_000 {
                *has_large_file = true;
            }

            let file_name = name.to_uppercase();
            
            if file_name.ends_with(".EFI") && file_name.contains("BOOT") {
                *supports_uefi = true;

                if let Ok(efi_bytes) = iso.read_file(&entry) {
                    if let Ok(pe) = pelite::PeFile::from_bytes(&efi_bytes) {
                        if let Ok(security) = pe.security() {
                            if !security.certificate_data().is_empty() {
                                *secure_boot_signed = true;
                            }
                        }
                    }
                }
            }

            if file_name == "INSTALL.WIM" {
                *is_windows = true;
                *install_image_type = "wim".to_string();
            } else if file_name == "INSTALL.ESD" {
                *is_windows = true;
                *install_image_type = "esd".to_string();
            } else if file_name == "INSTALL.SWM" {
                *is_windows = true;
                *install_image_type = "swm".to_string();
            }

            if file_name == "APPRAISERRES.DLL" {
                *is_windows_11 = true;
            }
        }
    }
}


#[pyfunction]
pub fn inspect_image(path: String) -> PyResult<ImageReport> {
    let file_path = Path::new(&path);
    
    if !file_path.exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
            "Image file not found: {}", path
        )));
    }

    let metadata = std::fs::metadata(file_path)?;
    let size_bytes = metadata.len();
    let mut file = File::open(file_path)?;

    let mut boot_sector = [0u8; 512];
    file.read_exact(&mut boot_sector)?;
    let is_isohybrid = boot_sector[510] == 0x55 && boot_sector[511] == 0xAA;

    file.seek(SeekFrom::Start(32768))?;
    let mut pvd = [0u8; 2048];
    file.read_exact(&mut pvd)?;

    let mut volume_label = String::from("UNKNOWN_ISO");
    if &pvd[1..6] == b"CD001" {
        let label_bytes = &pvd[40..72];
        volume_label = String::from_utf8_lossy(label_bytes).trim().to_string();
    }

    let mut has_large_file = false;
    let mut supports_uefi = false;
    let mut secure_boot_signed = false;
    let mut is_windows = false;
    let mut is_windows_11 = false;
    let mut install_image_type = String::new();

    file.seek(SeekFrom::Start(0))?;

    if let Ok(iso) = IsoImage::open(file) {
        let root = iso.root_dir();
        
        scan_directory(
            &iso, 
            root.dir_ref(), 
            &mut has_large_file, 
            &mut supports_uefi, 
            &mut secure_boot_signed,
            &mut is_windows,
            &mut is_windows_11,
            &mut install_image_type
        );
    }

    let boot_info = BootCapabilities {
        is_bootable: is_isohybrid || supports_uefi,
        supports_bios: is_isohybrid,
        supports_uefi,
        secure_boot_signed, 
    };

    let windows_info = if is_windows {
        Some(WindowsMetadata {
            is_windows: true,
            is_windows_11,
            install_image_type: install_image_type.clone(),
            // Windows To Go generally requires WIM or ESD images (SWM files are split and harder to apply)
            supports_wintogo: install_image_type == "wim" || install_image_type == "esd",
        })
    } else {
        None
    };

    Ok(ImageReport {
        file_path: path,
        size_bytes,
        volume_label,
        is_isohybrid,
        has_large_file, 
        boot_info,
        windows_info,
    })
}