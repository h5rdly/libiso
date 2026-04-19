use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::DirectoryRef;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::signature::inspect_secure_boot;
use crate::esd::{parse_wim_xml};

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
    #[pyo3(get)]
    pub is_microsoft_signed: bool, 
    #[pyo3(get)]
    pub is_revoked: bool,
    #[pyo3(get)]
    pub signature_size: usize, 
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
    // --- NEW FIELDS ---
    #[pyo3(get)]
    pub architecture: String,
    #[pyo3(get)]
    pub editions: Vec<String>,
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
        dict.set_item("is_microsoft_signed", self.is_microsoft_signed)?;
        dict.set_item("is_revoked", self.is_revoked)?;
        dict.set_item("signature_size", self.signature_size)?;
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
        dict.set_item("architecture", &self.architecture)?;
        dict.set_item("editions", &self.editions)?;
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
        
        dict.set_item("boot_info", self.boot_info.as_dict(py)?)?;
        
        if let Some(win_info) = &self.windows_info {
            dict.set_item("windows_info", win_info.as_dict(py)?)?;
        } else {
            dict.set_item("windows_info", py.None())?;
        }
        
        Ok(dict)
    }

    fn __str__(&self) -> String {
        let sb_str = if self.boot_info.secure_boot_signed {
            let ms_trust = if self.boot_info.is_microsoft_signed { "Microsoft Trusted" } else { "Unknown CA" };
            let revoked = if self.boot_info.is_revoked { " [VULNERABLE/REVOKED!]" } else { "" };
            format!("True ({} - {} bytes){}", ms_trust, self.boot_info.signature_size, revoked)
        } else {
            "False".to_string()
        };

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
            sb_str
        );

        s.push_str("\n--- Windows Metadata ---\n");
        if let Some(win) = &self.windows_info {
            s.push_str(&format!(
                "Is Windows:        Yes (Win 11: {})\n\
                 Image Type:        {}\n\
                 Architecture:      {}\n\
                 Editions:          {}\n\
                 WinToGo Supported: {}\n",
                win.is_windows_11, win.install_image_type.to_uppercase(), 
                win.architecture.to_uppercase(), win.editions.join(", "), 
                win.supports_wintogo
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

// Traverse the ISO Directory Tree
#[allow(clippy::too_many_arguments)]
fn scan_directory(
    iso: &IsoImage<File>, 
    dir_ref: DirectoryRef,
    has_large_file: &mut bool,
    supports_uefi: &mut bool, 
    is_windows: &mut bool,
    is_windows_11: &mut bool,
    install_image_type: &mut String,
) {
    let dir = iso.open_dir(dir_ref); 
    
    for entry_res in dir.entries() {
        let Ok(entry) = entry_res else { continue };
        
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" {
            continue;
        }

        let mut name = if entry.record.is_joliet_name() {
            entry.record.joliet_name()
        } else {
            entry.display_name().into_owned()
        };

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
pub fn inspect_image(file_path: String) -> PyResult<ImageStats> {
    let mut file = File::open(&file_path)?;
    let total_size = file.metadata()?.len();

    // 1. MAGIC NUMBER ROUTING
    // Read the first 8 bytes to check if it's a WIM or ESD file
    let mut magic = [0u8; 8];
    if file.read_exact(&mut magic).is_ok() {
        if &magic == b"MSWIM\x00\x00\x00" || &magic == b"WLPWM\x00\x00\x00" {
            // It's a WIM/ESD! Parse the XML metadata.
            if let Some(wim_info) = parse_wim_xml(total_size, |buf, offset| {
                file.seek(SeekFrom::Start(offset)).is_ok() && file.read_exact(buf).is_ok()
            }) {
                return Ok(ImageStats {
                    file_path, 
                    size_bytes: total_size,
                    volume_label: "WIM/ESD Archive".to_string(), // Dummy label for standalone archives
                    is_isohybrid: false,
                    has_large_file: true, // WIMs are basically guaranteed to have large solid chunks
                    boot_info: BootCapabilities {
                        is_bootable: false,
                        supports_bios: false,
                        supports_uefi: false,
                        secure_boot_signed: false, 
                        is_microsoft_signed: false,
                        is_revoked: false,
                        signature_size: 0,
                    },
                    windows_info: Some(WindowsMetadata {
                        is_windows: true,
                        is_windows_11: false, // appraiserres.dll isn't inside the WIM
                        install_image_type: "ESD/WIM".to_string(),
                        supports_wintogo: true,
                        architecture: wim_info.architecture.unwrap_or_else(|| "Unknown".to_string()),
                        editions: wim_info.editions,
                    }),
                });
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err("Invalid or corrupted WIM/ESD file"));
            }
        }
    }

    // 2. FALLBACK TO ISO9660 PARSING
    // Rewind the file back to 0 so we don't mess up our sector alignments
    file.seek(SeekFrom::Start(0))?;

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
    let mut signature_size = 0usize;
    let mut is_microsoft_signed = false; 
    let mut is_revoked = false;
    let mut is_windows = false;
    let mut is_windows_11 = false;
    let mut install_image_type = String::new();

    file.seek(SeekFrom::Start(0))?;

    if let Ok(iso) = IsoImage::open(file) {
        
        let sb_status = inspect_secure_boot(&iso);
        supports_uefi = sb_status.has_efi_bootloader;
        secure_boot_signed = sb_status.is_signed;
        is_microsoft_signed = sb_status.is_microsoft_signed;
        is_revoked = sb_status.is_revoked;
        signature_size = sb_status.signature_size;

        let root = iso.root_dir();
        
        scan_directory(
            &iso, root.dir_ref(), 
            &mut has_large_file, 
            &mut supports_uefi,
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
        is_microsoft_signed, 
        is_revoked,
        signature_size,
    };

    let windows_info = if is_windows {
        Some(WindowsMetadata {
            is_windows: true,
            is_windows_11,
            install_image_type: install_image_type.clone(),
            supports_wintogo: install_image_type == "wim" || install_image_type == "esd",
            // Since we don't parse the internal WIM XML during a full ISO scan (for speed), 
            // these are left empty/unknown for ISO files.
            architecture: String::new(), 
            editions: Vec::new(),
        })
    } else {
        None
    };

    Ok(ImageStats {
        file_path,
        size_bytes: total_size,
        volume_label,
        is_isohybrid,
        has_large_file, 
        boot_info,
        windows_info,
    })
}