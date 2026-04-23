use std::io::{Write};

use fatfs::{Dir, ReadWriteSeek, TimeProvider, OemCpConverter}; 
use edera_sprout_parsing::{match_kernel_prefix, LINUX_KERNEL_PREFIXES, 
    LINUX_INITRAMFS_PREFIXES};  // initramfs_candidates

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;


// Embed both Sprout binaries directly into libiso
const SPROUT_X86_64: &[u8] = include_bytes!("../libiso/sprout_0-0-28_x86_64.efi");
const SPROUT_AARCH64: &[u8] = include_bytes!("../libiso/sprout_0-0-28_aarch64.efi");


// Installs the Sprout UEFI bootloader onto the root of the FAT32/exFAT partition
pub fn install_uefi_sprout<T, TP, OCC>(
    root_dir: &Dir<'_, T, TP, OCC>, target_arch: &str, // Expects "x86_64", "aarch64", or "all"
) -> PyResult<()> 
where 
    T: ReadWriteSeek<Error = std::io::Error>,
    TP: TimeProvider,
    OCC: OemCpConverter,
{

    let efi_dir = root_dir.create_dir("EFI").unwrap_or_else(|_| root_dir.open_dir("EFI").unwrap());
    let boot_dir = efi_dir.create_dir("BOOT").unwrap_or_else(|_| efi_dir.open_dir("BOOT").unwrap());

    if target_arch == "x86_64" || target_arch == "all" {
        let mut sprout_x64 = boot_dir.create_file("BOOTX64.EFI").map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create BOOTX64.EFI: {:?}", e))
        })?;
        sprout_x64.truncate().unwrap();
        sprout_x64.write_all(SPROUT_X86_64).unwrap();
    }

    if target_arch == "aarch64" || target_arch == "all" {
        let mut sprout_aa64 = boot_dir.create_file("BOOTAA64.EFI").map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create BOOTAA64.EFI: {:?}", e))
        })?;
        sprout_aa64.truncate().unwrap();
        sprout_aa64.write_all(SPROUT_AARCH64).unwrap();
    }

    Ok(())
}


// Helper to scan an extracted filename to see if it's a Linux kernel or initramfs
pub fn detect_linux_payloads(
    filename: &str,
    current_path: &str,
    found_kernel: &mut Option<String>,
    found_initrd: &mut Option<String>
) {
    let lower_name = filename.to_lowercase();
    
    // Blacklist utility payloads / aux kernels so they don't overwrite the real OS kernel
    if lower_name.contains("memtest") || lower_name.contains("rescue") {
        return;
    }

    // If we haven't found a kernel yet, check if this file is one
    if found_kernel.is_none() {
        if match_kernel_prefix(&lower_name, LINUX_KERNEL_PREFIXES).is_some() {
            *found_kernel = Some(format!("{}/{}", current_path, filename));
            return;
        }
    }

    // If we haven't found an initrd, check if this file matches common initramfs patterns
    if found_initrd.is_none() {
        for prefix in LINUX_INITRAMFS_PREFIXES {
            if lower_name.starts_with(prefix) {
                *found_initrd = Some(format!("{}/{}", current_path, filename));
                return;
            }
        }
    }
}


// Scrapes GRUB or Syslinux configuration text to find default boot arguments
pub fn scrape_boot_args(config_content: &str, found_args: &mut Option<String>) {
    
    if found_args.is_some() {
        return; 
    }

    for line in config_content.lines() {
        let trimmed = line.trim();
        
        // Match GRUB style: "linux /casper/vmlinuz boot=casper quiet splash"
        if trimmed.starts_with("linux ") || trimmed.starts_with("linuxefi ") || trimmed.starts_with("linux16 ") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() > 2 {
                // Skip the "linux" command and the "/path/to/kernel"
                *found_args = Some(parts[2..].join(" "));
                return;
            }
        } 
        // Match Syslinux style: "APPEND boot=casper initrd=/casper/initrd.lz quiet splash"
        else if trimmed.starts_with("APPEND ") || trimmed.starts_with("append ") {
            let parts: Vec<&str> = trimmed.split_whitespace()
                .skip(1) // Skip the "APPEND" command
                .filter(|p| !p.starts_with("initrd=")) // Strip out initrd mapping, Sprout handles this!
                .collect();
                
            if !parts.is_empty() {
                *found_args = Some(parts.join(" "));
                return;
            }
        }
    }
}


// Generates and writes the explicit sprout.toml configuration file
pub fn write_sprout_toml<T, TP, OCC>(
    root_dir: &Dir<'_, T, TP, OCC>, 
    kernel_path: Option<&str>, 
    initrd_path: Option<&str>, 
    kernel_args: Option<&str>, 
    os_name: &str 
) -> PyResult<()> 
where 
    T: ReadWriteSeek<Error = std::io::Error>,
    TP: TimeProvider,
    OCC: OemCpConverter,
{
    
    let mut toml = String::new();
    toml.push_str("version = 1\n\n");
    
    // Even if we are missing paths, we can fallback to Sprout's autoconfigure
    if kernel_path.is_none() {
        toml.push_str("[options]\nautoconfigure = true\n");
    } else {
        toml.push_str("[entries.linux-iso]\n");
        toml.push_str(&format!("title = \"Boot Linux ISO - {os_name}\"\n"));
        toml.push_str("actions = [\"boot-linux\"]\n\n");

        toml.push_str("[actions.boot-linux.chainload]\n");
        
        if let Some(k) = kernel_path {
            // Sprout expects UEFI-style backslash paths
            let efi_kernel_path = k.replace('/', "\\");
            toml.push_str(&format!("path = \"{}\"\n", efi_kernel_path));
        }
        
        if let Some(i) = initrd_path {
            let efi_initrd_path = i.replace('/', "\\");
            toml.push_str(&format!("linux-initrd = \"{}\"\n", efi_initrd_path));
        }
        
        let args = kernel_args.unwrap_or("quiet splash");
        toml.push_str(&format!("options = [\"{}\"]\n", args));
    }

    let mut config_file = root_dir.create_file("sprout.toml").map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to create sprout.toml: {:?}", e))
    })?;

    config_file.truncate().unwrap();
    config_file.write_all(toml.as_bytes()).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to write sprout.toml: {:?}", e))
    })?;

    Ok(())
}