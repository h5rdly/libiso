use std::path::Path;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::DirectoryRef;

use pyo3::prelude::*;
use pyo3::types::PyDict;


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
pub struct ImageStats {
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


#[pymethods]
impl BootCapabilities {
    pub fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("is_bootable", self.is_bootable)?;
        dict.set_item("supports_bios", self.supports_bios)?;
        dict.set_item("supports_uefi", self.supports_uefi)?;
        dict.set_item("secure_boot_signed", self.secure_boot_signed)?;
        Ok(dict)
    }
}

#[pymethods]
impl WindowsMetadata {
    pub fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("is_windows", self.is_windows)?;
        dict.set_item("is_windows_11", self.is_windows_11)?;
        dict.set_item("install_image_type", &self.install_image_type)?;
        dict.set_item("supports_wintogo", self.supports_wintogo)?;
        Ok(dict)
    }
}

#[pymethods]
impl ImageStats {
    pub fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("file_path", &self.file_path)?;
        dict.set_item("size_bytes", self.size_bytes)?;
        dict.set_item("volume_label", &self.volume_label)?;
        dict.set_item("is_isohybrid", self.is_isohybrid)?;
        dict.set_item("has_large_file", self.has_large_file)?;
        
        // Nest the dictionaries
        dict.set_item("boot_info", self.boot_info.as_dict(py)?)?;
        
        if let Some(win_info) = &self.windows_info {
            dict.set_item("windows_info", win_info.as_dict(py)?)?;
        } else {
            dict.set_item("windows_info", py.None())?;
        }
        
        Ok(dict)
    }

    fn __str__(&self) -> String {
        let mut s = format!(
            "Volume Label:      {}\n\
             Size:              {} bytes\n\
             ISOHybrid:         {}\n\
             Large File (>4GB): {}\n\n\
             --- Boot Info ---\n\
             Bootable:          {} (BIOS: {}, UEFI: {})\n\
             Secure Boot:       {}\n",
            self.volume_label, self.size_bytes, self.is_isohybrid, self.has_large_file,
            self.boot_info.is_bootable, self.boot_info.supports_bios, self.boot_info.supports_uefi,
            self.boot_info.secure_boot_signed
        );

        s.push_str("\n--- Windows Metadata ---\n");
        if let Some(win) = &self.windows_info {
            s.push_str(&format!(
                "Is Windows:        Yes (Win 11: {})\n\
                 Image Type:        {}\n\
                 WinToGo Supported: {}\n",
                win.is_windows_11, win.install_image_type.to_uppercase(), win.supports_wintogo
            ));
        } else {
            s.push_str("Is Windows:        False\n");
        }
        s
    }
    
    fn __repr__(&self) -> String {
        format!("<ImageStats volume_label='{}' size_bytes={}>", self.volume_label, self.size_bytes)
    }
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

        // Extract the true name using Rock Ridge or Joliet decoding
        let mut name = if entry.record.is_joliet_name() {
            entry.record.joliet_name() // Decode Windows UTF-16
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
pub fn inspect_image(path: String) -> PyResult<ImageStats> {
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

    Ok(ImageStats {
        file_path: path,
        size_bytes,
        volume_label,
        is_isohybrid,
        has_large_file, 
        boot_info,
        windows_info,
    })
}