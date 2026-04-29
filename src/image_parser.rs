use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use hadris_iso::{sync::IsoImage, directory::DirectoryRef};

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::signature::{inspect_secure_boot, inspect_secure_boot_bytes};
use crate::esd::{parse_wim_xml};
use crate::udf;


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
    pub is_unpatchable_linux: bool,
    #[pyo3(get)]
    pub linux_kernel_version: Option<String>,

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
        dict.set_item("is_unpatchable_linux", self.is_unpatchable_linux)?; 
        dict.set_item("boot_info", self.boot_info.as_dict(py)?)?;
        if let Some(win_info) = &self.windows_info {
            dict.set_item("windows_info", win_info.as_dict(py)?)?;
        } else {
            dict.set_item("windows_info", py.None())?;
        }
        if let Some(version) = &self.linux_kernel_version {
            dict.set_item("linux_kernel_version", version)?;
        } else {
            dict.set_item("linux_kernel_version", py.None())?;
        }
        Ok(dict)
        
    }
}


pub fn scan_directory_udf(
    file: &mut File,
    partition_start: u32,
    entries: &[udf::UdfDirEntry],
    has_large_file: &mut bool,
    supports_uefi: &mut bool, 
    is_windows: &mut bool,
    is_windows_11: &mut bool,
    install_image_type: &mut String,
    is_unpatchable_linux: &mut bool, 
) {
    for entry in entries {
        let name = &entry.name;
        if name == "." || name == ".." { continue; }
        let file_name = name.split(';').next().unwrap_or(name).to_uppercase();

        if file_name.ends_with(".MISO") || file_name.contains("POP-OS") || file_name.contains("POP_OS") {
            *is_unpatchable_linux = true;
        }

        if entry.is_directory {
            if let Ok(sub_entries) = udf::read_directory(file, partition_start, &entry.icb) {
                scan_directory_udf(
                    file, partition_start, &sub_entries, has_large_file, supports_uefi, 
                    is_windows, is_windows_11, install_image_type, is_unpatchable_linux
                );
            }
        } else {
            // Because our custom parser ignores size to speed up parsing,
            // we will let the install type dictate the large file flag
            
            if file_name.ends_with(".EFI") && file_name.contains("BOOT") {
                *supports_uefi = true;
            }
            if file_name == "INSTALL.WIM" {
                *is_windows = true;
                *has_large_file = true; // WIMs are basically always >4GB
                *install_image_type = "wim".to_string();
            } else if file_name == "INSTALL.ESD" {
                *is_windows = true;
                *has_large_file = true;
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


#[allow(clippy::too_many_arguments)]
fn scan_directory(
    iso: &IsoImage<File>, 
    dir_ref: DirectoryRef,
    has_large_file: &mut bool,
    supports_uefi: &mut bool, 
    is_windows: &mut bool,
    is_windows_11: &mut bool,
    install_image_type: &mut String,
    is_unpatchable_linux: &mut bool, 
) {
    let dir = iso.open_dir(dir_ref); 
    for entry_res in dir.entries() {
        let Ok(entry) = entry_res else { continue };
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" { continue; }

        let mut name = if entry.record.is_joliet_name() {
            entry.record.joliet_name()
        } else {
            entry.display_name().into_owned()
        };

        if let Some(pos) = name.rfind(';') { name.truncate(pos); }
        let file_name = name.to_uppercase();
        if file_name.ends_with(".MISO") || file_name.contains("POP-OS") || file_name.contains("POP_OS") {
            *is_unpatchable_linux = true;
        }   
        if entry.is_directory() {
            if let Ok(sub_dir_ref) = entry.as_dir_ref(iso) {
                scan_directory(iso, sub_dir_ref, has_large_file, supports_uefi, is_windows,
                    is_windows_11, install_image_type, is_unpatchable_linux);
            }
        } else {
            if entry.total_size() >= 4_000_000_000 { *has_large_file = true; }
            if file_name.ends_with(".EFI") && file_name.contains("BOOT") { *supports_uefi = true; }
            
            // bzimage header scraping
            if file_name.contains("VMLINUZ") || file_name.contains("BZIMAGE") || file_name == "LINUX"
            {
                // ISO sectors are exactly 2048 bytes
                let start_sector = entry.header().extent.read() as u64;
                let byte_offset = start_sector * 2048;
                
                // Read exactly 8192 bytes (or the file size if it's somehow smaller)
                let read_size = std::cmp::min(8192, entry.total_size()) as usize;
                let mut header_buf = vec![0u8; read_size];
                
                // hadris-iso direct seek-and-read
                if iso.read_bytes_at(byte_offset, &mut header_buf).is_ok() {
                    let _linux_kernel_version = extract_bzimage_version(&header_buf);
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
            if file_name == "APPRAISERRES.DLL" { *is_windows_11 = true; }
        }
    }
}


#[pyfunction]
pub fn inspect_image(file_path: String) -> PyResult<ImageStats> {
    let mut file = File::open(&file_path)?;
    let total_size = file.metadata()?.len();

    let mut magic = [0u8; 8];
    if file.read_exact(&mut magic).is_ok() {
        if &magic == b"MSWIM\x00\x00\x00" || &magic == b"WLPWM\x00\x00\x00" {
            if let Some(wim_info) = parse_wim_xml(total_size, |buf, offset| {
                file.seek(SeekFrom::Start(offset)).is_ok() && file.read_exact(buf).is_ok()
            }) {
                return Ok(ImageStats {
                    file_path, 
                    size_bytes: total_size,
                    volume_label: "WIM/ESD Archive".to_string(), 
                    is_isohybrid: false,
                    has_large_file: true, 
                    is_unpatchable_linux: false, 
                    linux_kernel_version: None,
                    boot_info: BootCapabilities {
                        is_bootable: false, supports_bios: false, supports_uefi: false,
                        secure_boot_signed: false, is_microsoft_signed: false, is_revoked: false, signature_size: 0,
                    },
                    windows_info: Some(WindowsMetadata {
                        is_windows: true, is_windows_11: false, install_image_type: "ESD/WIM".to_string(),
                        supports_wintogo: true, architecture: wim_info.architecture.unwrap_or_else(|| "Unknown".to_string()),
                        editions: wim_info.editions,
                    }),
                });
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err("Invalid or corrupted WIM/ESD file"));
            }
        }
    }

    file.seek(SeekFrom::Start(0))?;
    let mut boot_sector = [0u8; 512];
    file.read_exact(&mut boot_sector)?;
    let is_isohybrid = boot_sector[510] == 0x55 && boot_sector[511] == 0xAA;

    file.seek(SeekFrom::Start(32768))?;
    let mut pvd = [0u8; 2048];
    file.read_exact(&mut pvd)?;
    let mut volume_label = String::from("UNKNOWN_ISO");
    if &pvd[1..6] == b"CD001" {
        volume_label = String::from_utf8_lossy(&pvd[40..72]).trim().to_string();
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
    let mut is_unpatchable_linux = false; 
    let linux_kernel_version = None;


    file.seek(SeekFrom::Start(0))?;
    let mut is_udf_valid = false;

    if let Ok(udf_ctx) = udf::mount_udf(&mut file) {
        if let Ok(root_entries) = udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb) {
            is_udf_valid = true;
            let udf_label = udf_ctx.volume_id.trim();
            if !udf_label.is_empty() { volume_label = udf_label.to_string(); }
            
            scan_directory_udf(
                &mut file, udf_ctx.partition_start, &root_entries, &mut has_large_file, 
                &mut supports_uefi, &mut is_windows, &mut is_windows_11, &mut install_image_type,
                &mut is_unpatchable_linux
            );
            
            if let Some(entry) = udf::find_udf_entry(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb, "EFI/BOOT/BOOTX64.EFI")
                .or_else(|| udf::find_udf_entry(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb, "BOOTX64.EFI")) 
            {
                if let Ok(bytes) = udf::read_file_bytes(&mut file, udf_ctx.partition_start, &entry) {
                    let sb_status = inspect_secure_boot_bytes(&bytes);
                    supports_uefi = sb_status.has_efi_bootloader;
                    secure_boot_signed = sb_status.is_signed;
                    is_microsoft_signed = sb_status.is_microsoft_signed;
                    is_revoked = sb_status.is_revoked;
                    signature_size = sb_status.signature_size;
                }
            }
        }
    }

    if !is_udf_valid {
        file.seek(SeekFrom::Start(0))?;
        if let Ok(iso) = IsoImage::open(file) {
            let sb_status = inspect_secure_boot(&iso);
            supports_uefi = sb_status.has_efi_bootloader;
            secure_boot_signed = sb_status.is_signed;
            is_microsoft_signed = sb_status.is_microsoft_signed;
            is_revoked = sb_status.is_revoked;
            signature_size = sb_status.signature_size;

            let root = iso.root_dir();
            scan_directory(&iso, root.dir_ref(), &mut has_large_file, &mut supports_uefi, &mut is_windows, 
            &mut is_windows_11, &mut install_image_type, &mut is_unpatchable_linux);
        }
    }

    let boot_info = BootCapabilities {
        is_bootable: is_isohybrid || supports_uefi, supports_bios: is_isohybrid, supports_uefi,
        secure_boot_signed, is_microsoft_signed, is_revoked, signature_size,
    };

    let windows_info = if is_windows {
        Some(WindowsMetadata {
            is_windows: true, is_windows_11, install_image_type: install_image_type.clone(),
            supports_wintogo: install_image_type == "wim" || install_image_type == "esd",
            architecture: String::new(), editions: Vec::new(),
        })
    } else { None };

    Ok(ImageStats { file_path, size_bytes: total_size, volume_label, is_isohybrid, has_large_file, 
        is_unpatchable_linux, linux_kernel_version, boot_info, windows_info })
}


// Extract the human-readable kernel version string from a Linux bzImage file
pub fn extract_bzimage_version(bzimage: &[u8]) -> Option<String> {
    
    // The image must be at least large enough to contain the setup header
    if bzimage.len() < 0x0210 {
        return None;
    }

    // Check for "HdrS" magic signature at offset 0x0202
    if &bzimage[0x0202..0x0206] != b"HdrS" {
        return None; 
    }

    // Read the 16-bit pointer to the kernel version string at 0x020E
    let version_ptr = u16::from_le_bytes([bzimage[0x020E], bzimage[0x020F]]);

    // The pointer is an offset from 0x0200
    let absolute_offset = 0x0200 + version_ptr as usize;

    if absolute_offset >= bzimage.len() {
        return None;
    }

    // Read until we hit a null byte (0x00)
    let mut end_offset = absolute_offset;
    while end_offset < bzimage.len() && bzimage[end_offset] != 0 {
        end_offset += 1;
    }

    // Extract and convert to a Rust String (lossy handles weird encoding gracefully)
    let version_slice = &bzimage[absolute_offset..end_offset];
    
    let version_str = String::from_utf8_lossy(version_slice).trim().to_string();
    
    if version_str.is_empty() { None } else { Some(version_str) }
}