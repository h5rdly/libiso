use pyo3::prelude::*;


#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct DriveInfo {
    #[pyo3(get)]
    pub display_name: String,
    
    #[pyo3(get)]
    pub device_path: String, 
    
    #[pyo3(get)]
    pub total_space_bytes: u64,
}


#[pymethods]
impl DriveInfo {
    #[new]
    pub fn new(display_name: String, device_path: String, total_space_bytes: u64) -> Self {
        DriveInfo {
            display_name, device_path, total_space_bytes,
        }
    }
}

// Linux specific implementation - read raw block devices
#[cfg(target_os = "linux")]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    let mut available_drives = Vec::new();
    let hardware_devices = drives::get_devices().unwrap_or_default();

    for device in hardware_devices {
        if !device.is_removable {
            continue;
        }

        let hw_name = device.model.unwrap_or_else(|| "Generic USB Drive".to_string());
        let device_path = format!("/dev/{}", device.name);
        
        let size_path = format!("/sys/block/{}/size", device.name);
        let total_space_bytes = std::fs::read_to_string(&size_path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|sectors| sectors * 512) // 1 sector = 512 bytes
            .unwrap_or(0);
            
        let size_gb = total_space_bytes / (1024 * 1024 * 1024);

        let display_name = format!("{} - {} GB ({})", hw_name.trim(), size_gb, device_path);

        available_drives.push(DriveInfo {
            display_name,
            device_path,
            total_space_bytes, 
        });
    }

    available_drives
}


// Win / Mac implementation - read OS volume mounts
#[cfg(not(target_os = "linux"))]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    use sysinfo::Disks;
    
    let mut available_drives = Vec::new();
    let disks = Disks::new_with_refreshed_list();

    for disk in disks.list() {
        if !disk.is_removable() || disk.is_read_only() {
            continue;
        }

        let hw_name = disk.name().to_string_lossy().into_owned();
        let device_path = disk.mount_point().to_string_lossy().into_owned();
        
        let total_space_bytes = disk.total_space();
        let size_gb = total_space_bytes / (1024 * 1024 * 1024);

        let display_name = format!("{} - {} GB ({})", hw_name, size_gb, device_path);

        available_drives.push(DriveInfo {
            display_name,
            device_path,
            total_space_bytes,
        });
    }

    available_drives
}


