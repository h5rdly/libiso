use pyo3::prelude::*;
use pyo3::types::PyDict;



#[pyclass(skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct DriveInfo {
    #[pyo3(get)]
    pub display_name: String, 
    #[pyo3(get)]
    pub device_path: String, 
    
    #[pyo3(get)]
    pub total_space_bytes: u64,

    #[pyo3(get)]
    pub label: Option<String>,
    
    #[pyo3(get)]
    pub hardware_model: String,
}

#[pymethods]
impl DriveInfo {
    #[new]
    pub fn new(display_name: String, device_path: String, total_space_bytes: u64, label: Option<String>, hardware_model: String) -> Self {
        DriveInfo {
            display_name, device_path, total_space_bytes, label, hardware_model
        }
    }

    pub fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("display_name", &self.display_name)?;
        dict.set_item("device_path", &self.device_path)?;
        dict.set_item("total_space_bytes", self.total_space_bytes)?;
        dict.set_item("hardware_model", &self.hardware_model)?;
        
        if let Some(l) = &self.label {
            dict.set_item("label", l)?;
        } else {
            dict.set_item("label", py.None())?;
        }
        Ok(dict)
    }
}


// -- Linux  
#[cfg(target_os = "linux")]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {

    use std::{fs, path::Path};

    let mut available_drives = Vec::new();
    let block_dir = Path::new("/sys/block");
    
    if let Ok(entries) = fs::read_dir(block_dir) {
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().into_string().unwrap_or_default();
            
            // We only want actual physical drives, not loopbacks or ramdisks
            if !name.starts_with("sd") && !name.starts_with("nvme") && !name.starts_with("mmcblk") {
                continue;
            }

            let path = entry.path();
            
            // Check if it's removable (USB)
            let removable_str = fs::read_to_string(path.join("removable")).unwrap_or_default();
            let is_removable = removable_str.trim() == "1";

            if !is_removable { continue; }

            // Get total size (sysfs exposes size in 512-byte sectors)
            let size_str = fs::read_to_string(path.join("size")).unwrap_or_default();
            let size_sectors: u64 = size_str.trim().parse().unwrap_or(0);
            let total_space_bytes = size_sectors * 512;

            if total_space_bytes == 0 { continue; }

            // Get Hardware Model
            let model = fs::read_to_string(path.join("device/model"))
                .unwrap_or_else(|_| "Generic USB Drive".to_string())
                .trim()
                .to_string();

            let device_path = format!("/dev/{}", name);

            available_drives.push(DriveInfo {
                display_name: format!("{} ({})", model, device_path),
                device_path,
                total_space_bytes,
                label: None, // Raw devices don't have labels, only partitions do!
                hardware_model: model,
            });
        }
    }
    available_drives
}


// -- MacOS (Using subprocess) 
#[cfg(target_os = "macos")]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    
    let mut available_drives = Vec::new();
    let output = std::process::Command::new("diskutil")
        .args(&["list", "external", "physical"])
        .output();

    if let Ok(out) = output {
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            if line.starts_with("/dev/disk") && line.contains("external") {
                let path = line.split_whitespace().next().unwrap_or("").to_string();
                
                let hardware_model = "External Mac Drive".to_string();
                available_drives.push(DriveInfo {
                    display_name: format!("{} ({})", hardware_model, path),
                    device_path: path,
                    total_space_bytes: 10_000_000_000, // Parse `diskutil info` if actual size is strictly needed
                    label: None,
                    hardware_model,
                });
            }
        }
    }
    available_drives
}


// -- Windows
#[cfg(target_os = "windows")]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    use std::os::windows::ffi::OsStrExt;
    use std::ffi::OsStr;
    use crate::io::sys::*; 

    let mut drives = Vec::new();

    // Scan the first 64 physical drives attached to the system
    for i in 0..64 {
        let path_str = format!("\\\\.\\PhysicalDrive{}", i);
        let wide_path: Vec<u16> = OsStr::new(&path_str).encode_wide().chain(std::iter::once(0)).collect();

        let handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                GENERIC_READ, 
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                std::ptr::null_mut(),
                OPEN_EXISTING,
                0,
                std::ptr::null_mut(),
            )
        };

        if handle == INVALID_HANDLE_VALUE {
            continue;
        }

        let mut query = STORAGE_PROPERTY_QUERY {
            PropertyId: 0, // StorageDeviceProperty
            QueryType: 0,  // PropertyStandardQuery
            AdditionalParameters: [0],
        };

        // Buffer for the descriptor and the C-strings that follow it
        let mut buffer = vec![0u8; 1024];
        let mut bytes_returned = 0;

        let success = unsafe {
            DeviceIoControl(
                handle,
                IOCTL_STORAGE_QUERY_PROPERTY,
                &mut query as *mut _ as *mut std::ffi::c_void,
                std::mem::size_of::<STORAGE_PROPERTY_QUERY>() as u32,
                buffer.as_mut_ptr() as *mut std::ffi::c_void,
                buffer.len() as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };

        if success != 0 {
            let descriptor = unsafe { &*(buffer.as_ptr() as *const STORAGE_DEVICE_DESCRIPTOR) };
            
            // BusType 7 = USB, BusType 12 = SD card. Or simply check if the OS flagged it as removable media.
            if descriptor.RemovableMedia != 0 || descriptor.BusType == 7 || descriptor.BusType == 12 {
                
                // Helper to extract C-strings from the buffer using the offsets provided by the Win32 struct
                let parse_str = |offset: u32| -> String {
                    if offset == 0 || offset as usize >= buffer.len() {
                        return String::new();
                    }
                    let start = offset as usize;
                    let end = buffer[start..].iter().position(|&c| c == 0).unwrap_or(0);
                    String::from_utf8_lossy(&buffer[start..start + end]).trim().to_string()
                };

                let vendor = parse_str(descriptor.VendorIdOffset);
                let product = parse_str(descriptor.ProductIdOffset);
                
                let mut hardware_model = format!("{} {}", vendor, product).trim().to_string();
                if hardware_model.is_empty() {
                    hardware_model = "Generic USB Drive".to_string();
                }

                // Query the exact byte capacity of the drive
                let mut size: i64 = 0;
                let size_success = unsafe {
                    DeviceIoControl(
                        handle,
                        IOCTL_DISK_GET_LENGTH_INFO,
                        std::ptr::null_mut(),
                        0,
                        &mut size as *mut _ as *mut std::ffi::c_void,
                        std::mem::size_of::<i64>() as u32,
                        &mut bytes_returned,
                        std::ptr::null_mut(),
                    )
                };

                let total_space_bytes = if size_success != 0 { size as u64 } else { 0 };

                if total_space_bytes > 0 {
                    drives.push(DriveInfo {
                        display_name: format!("{} ({})", hardware_model, path_str),
                        device_path: path_str,
                        total_space_bytes,
                        label: None,
                        hardware_model,
                    });
                }
            }
        }

        unsafe { CloseHandle(handle) };
    }

    drives
}


// -- FreeBSD
#[cfg(target_os = "freebsd")]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    use std::ffi::CString;

    // Manual FFI bindings to avoid needing the `libc` crate dependency
    extern "C" {
        fn sysctlbyname(
            name: *const std::ffi::c_char,
            oldp: *mut std::ffi::c_void,
            oldlenp: *mut usize,
            newp: *const std::ffi::c_void,
            newlen: usize,
        ) -> std::ffi::c_int;
    }

    let mut available_drives = Vec::new();

    // Ask the FreeBSD kernel for the length of the GEOM XML topology
    let mib = CString::new("kern.geom.confxml").unwrap();
    let mut len: usize = 0;
    
    unsafe {
        sysctlbyname(
            mib.as_ptr(),
            std::ptr::null_mut(),
            &mut len,
            std::ptr::null_mut(),
            0,
        );
    }

    if len == 0 { return available_drives; }

    // Allocate a buffer and fetch the actual XML string directly from kernel memory
    let mut buffer = vec![0u8; len];
    let res = unsafe {
        sysctlbyname(
            mib.as_ptr(),
            buffer.as_mut_ptr() as *mut std::ffi::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };

    if res != 0 { return available_drives; }

    let xml = String::from_utf8_lossy(&buffer);

    // Simple XML tag extractor helper
    let extract_tag = |block: &str, tag: &str| -> String {
        let open_tag = format!("<{}>", tag);
        let close_tag = format!("</{}>", tag);
        if let Some(start) = block.find(&open_tag) {
            if let Some(end) = block[start..].find(&close_tag) {
                return block[start + open_tag.len()..end].trim().to_string();
            }
        }
        String::new()
    };

    // Parse the GEOM XML for hardware providers
    let mut search_idx = 0;
    while let Some(provider_start) = xml[search_idx..].find("<provider ") {
        let abs_start = search_idx + provider_start;
        let abs_end = xml[abs_start..].find("</provider>").unwrap_or(0) + abs_start;
        if abs_end <= abs_start { break; }

        let provider_block = &xml[abs_start..abs_end];
        search_idx = abs_end;

        let name = extract_tag(provider_block, "name");
        
        // FreeBSD identifies USB mass storage as Direct Access (da). 
        // We only want root drives (da0, da1), ignoring partitions (da0p1, da0s1)
        if name.starts_with("da") && name.chars().skip(2).all(|c| c.is_ascii_digit()) {
            
            let size_bytes = extract_tag(provider_block, "mediasize").parse::<u64>().unwrap_or(0);
            let descr = extract_tag(provider_block, "descr");
            let hardware_model = if descr.is_empty() { "FreeBSD USB Drive".to_string() } else { descr };
            let device_path = format!("/dev/{}", name);

            if size_bytes > 0 {
                available_drives.push(DriveInfo {
                    display_name: format!("{} ({})", hardware_model, device_path),
                    device_path,
                    total_space_bytes: size_bytes,
                    label: None,
                    hardware_model,
                });
            }
        }
    }

    available_drives
}



// Placeholder for OpenBSD / NetBSD to pass CI
#[cfg(not(any(
    target_os = "windows",
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd"
)))]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    Vec::new()
}
