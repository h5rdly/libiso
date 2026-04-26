use std::io::Write;

use edera_sprout_parsing::{match_kernel_prefix, LINUX_KERNEL_PREFIXES, LINUX_INITRAMFS_PREFIXES}; 

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;

use crate::writer::UsbWriter;

// Embed  Sprout binaries directly into libiso
const SPROUT_X86_64: &[u8] = include_bytes!("../libiso/sprout_0-0-28_x86_64.efi");
const SPROUT_AARCH64: &[u8] = include_bytes!("../libiso/sprout_0-0-28_aarch64.efi");


// Installs the Sprout UEFI bootloader using our generic UsbWriter!
pub fn install_uefi_sprout<W: UsbWriter>(
    writer: &W, target_arch: &str, // Expects "x86_64", "aarch64", or "all"
) -> PyResult<()> {
    
    // We ignore errors here in case the directories already exist
    let _ = writer.create_dir("/EFI");
    let _ = writer.create_dir("/EFI/BOOT");

    if target_arch == "x86_64" || target_arch == "all" {
        let mut sprout_x64 = writer.open_file_writer("/EFI/BOOT/BOOTX64.EFI", SPROUT_X86_64.len() as u64).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create BOOTX64.EFI: {:?}", e))
        })?;
        sprout_x64.write_all(SPROUT_X86_64).unwrap();
        sprout_x64.flush().unwrap();
    }

    if target_arch == "aarch64" || target_arch == "all" {
        let mut sprout_aa64 = writer.open_file_writer("/EFI/BOOT/BOOTAA64.EFI", SPROUT_AARCH64.len() as u64).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create BOOTAA64.EFI: {:?}", e))
        })?;
        sprout_aa64.write_all(SPROUT_AARCH64).unwrap();
        sprout_aa64.flush().unwrap();
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


pub fn write_sprout_toml<W: UsbWriter>(
    writer: &W, 
    kernel_path: Option<&str>, 
    initrd_path: Option<&str>, 
    kernel_args: Option<&str>, 
    os_name: &str 
) -> PyResult<()> {
    
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
            toml.push_str(&format!("path = '{}'\n", efi_kernel_path));
        }
        
        if let Some(i) = initrd_path {
            let efi_initrd_path = i.replace('/', "\\");
            toml.push_str(&format!("linux-initrd = '{}'\n", efi_initrd_path));
        }
        
        let args = kernel_args.unwrap_or("quiet splash");
        toml.push_str(&format!("options = ['{}']\n", args));
    }
    let toml_bytes = toml.as_bytes();
    let mut config_file = writer.open_file_writer("/sprout.toml", toml_bytes.len() as u64).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to create sprout.toml: {:?}", e))
    })?;

    config_file.write_all(toml_bytes).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to write sprout.toml: {:?}", e))
    })?;
    config_file.flush().unwrap();

    Ok(())
}