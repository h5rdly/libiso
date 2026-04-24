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


#[cfg(target_os = "linux")]
#[pyfunction]
pub fn list_removable_drives() -> Vec<DriveInfo> {
    let mut available_drives = Vec::new();
    let hardware_devices = drives::get_devices().unwrap_or_default();

    for device in hardware_devices {
        if !device.is_removable { continue; }

        // Get the hardware model, trim trailing spaces that some USB firmware leaves
        let hw_name = device.model.unwrap_or_else(|| "Generic USB".to_string()).trim().to_string();
        let device_path = format!("/dev/{}", device.name);
        
        let mut label = None;
        if let Ok(out) = std::process::Command::new("lsblk")
            .args(&["-n", "-o", "LABEL", &device_path])
            .output() 
        {
            let out_str = String::from_utf8_lossy(&out.stdout);
            if let Some(l) = out_str.lines().map(|s| s.trim()).find(|s| !s.is_empty()) {
                label = Some(l.to_string());
            }
        }
        
        let size_path = format!("/sys/block/{}/size", device.name);
        let total_space_bytes = std::fs::read_to_string(&size_path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|sectors| sectors * 512)
            .unwrap_or(0);

        available_drives.push(DriveInfo {
            display_name: format!("{} ({})", hw_name, device_path), // Generic fallback
            device_path,
            total_space_bytes,
            label, 
            hardware_model: hw_name,
        });
    }
    available_drives
}


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

        let label_str = disk.name().to_string_lossy().into_owned();
        let label = if label_str.trim().is_empty() { None } else { Some(label_str.trim().to_string()) };

        let mut device_path = disk.mount_point().to_string_lossy().into_owned();
        #[cfg(target_os = "windows")]
        {
            if device_path.ends_with('\\') || device_path.ends_with('/') {
                device_path.pop(); 
            }
            device_path = format!("\\\\.\\{}", device_path);
        }
        
        let total_space_bytes = disk.total_space();
        
        // sysinfo doesn't easily expose physical hardware names (eg "Cruzer xx")
        let hardware_model = "USB Flash Drive".to_string(); 

        available_drives.push(DriveInfo {
            display_name: format!("{} ({})", hardware_model, device_path),
            device_path,
            total_space_bytes,
            label,
            hardware_model,
        });
    }
    available_drives
}