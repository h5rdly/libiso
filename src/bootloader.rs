use std::io::Write;

use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;

use crate::writer::UsbWriter;

// Embed  Sprout binaries directly into libiso
const SPROUT_X86_64: &[u8] = include_bytes!("../libiso/sprout_0-0-28_x86_64.efi");
const SPROUT_AARCH64: &[u8] = include_bytes!("../libiso/sprout_0-0-28_aarch64.efi");

const LINUX_KERNEL_PREFIXES: &[&str] = &[
    "vmlinuz", "bzimage", "image", "kernel", "kernel-"
    ];
const LINUX_INITRAMFS_PREFIXES: &[&str] = &[
    "initrd", "initramfs", "microcode", "ucode", "amd-ucode", "intel-ucode"
];


pub fn install_uefi_sprout<W: UsbWriter>(writer: &W, target_arch: &str,) -> PyResult<()> {
    
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
    filename: &str, current_path: &str, found_kernel: &mut Option<String>, found_initrd: &mut Option<String>
) {
    let lower_name = filename.to_lowercase();
    let lower_path = current_path.to_lowercase();
    
    // Blacklist utility payloads and bootloader modules
    if lower_name.contains("memtest") || lower_name.contains("rescue") || lower_path.contains("grub") {
        return;
    }

    if found_kernel.is_none() {
        if LINUX_KERNEL_PREFIXES.iter().any(|&prefix| lower_name.starts_with(prefix)) {
            // Prevent GRUB/Syslinux modules (like kernel.img or boot.efi) from being flagged as the Linux kernel
            if !lower_name.ends_with(".img") && !lower_name.ends_with(".efi") && !lower_name.ends_with(".sys") {
                *found_kernel = Some(format!("{}/{}", current_path, filename));
                return;
            }
        }
    }

    if found_initrd.is_none() {
        if LINUX_INITRAMFS_PREFIXES.iter().any(|&prefix| lower_name.starts_with(prefix)) {
            *found_initrd = Some(format!("{}/{}", current_path, filename));
            return;
        }
    }
}



pub fn patch_boot_labels(cfg: &str, new_label: &str) -> String {
    let mut result = cfg.to_string();
    
    // Patch kernel arguments (e.g., root=live:LABEL=Adelie-x86_64)
    let prefixes = ["LABEL=", "label=", "CDLABEL=", "archisolabel="];
    for prefix in prefixes {
        let mut current = 0;
        while let Some(idx) = result[current..].find(prefix) {
            let start = current + idx + prefix.len();
            let end_offset = result[start..]
                .find(|c: char| c == ' ' || c == '"' || c == '\'' || c == '\n' || c == '\r')
                .unwrap_or(result[start..].len());
            let end = start + end_offset;
            
            result = format!("{}{}{}", &result[..start], new_label, &result[end..]);
            current = start + new_label.len();
        }
    }
    
    // Patch GRUB search commands (e.g., search --label "Adelie-x86_64")
    let mut current = 0;
    let prefix = "--label ";
    while let Some(idx) = result[current..].find(prefix) {
        let start = current + idx + prefix.len();
        let has_quote = result[start..].starts_with('"') || result[start..].starts_with('\'');
        
        let val_start = if has_quote { start + 1 } else { start };
        let end_offset = if has_quote {
            let quote = result[start..].chars().next().unwrap();
            result[val_start..].find(quote).unwrap_or(result[val_start..].len())
        } else {
            result[val_start..].find(|c: char| c == ' ' || c == '\n' || c == '\r').unwrap_or(result[val_start..].len())
        };
        
        let end = val_start + end_offset;
        result = format!("{}{}{}", &result[..val_start], new_label, &result[end..]);
        current = val_start + new_label.len() + if has_quote { 1 } else { 0 };
    }

    result
}



pub fn scrape_boot_args(config_content: &str, found_args: &mut Option<String>, new_usb_label: &str) {
    if found_args.is_some() {
        return; 
    }

    for line in config_content.lines() {
        let trimmed = line.trim();
        let mut extracted_args = String::new();
        
        // Match GRUB style: "linux /casper/vmlinuz boot=casper quiet splash"
        if trimmed.starts_with("linux ") || trimmed.starts_with("linuxefi ") || trimmed.starts_with("linux16 ") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() > 2 {
                // Skip the "linux" command and the "/path/to/kernel"
                extracted_args = parts[2..].join(" ");
            }
        } 
        // Match Syslinux style: "APPEND boot=casper initrd=/casper/initrd.lz quiet splash"
        else if trimmed.starts_with("APPEND ") || trimmed.starts_with("append ") {
            let parts: Vec<&str> = trimmed.split_whitespace()
                .skip(1) // Skip the "APPEND" command
                .filter(|p| !p.starts_with("initrd=")) // Strip out initrd mapping, Sprout handles this!
                .collect();
                
            if !parts.is_empty() {
                extracted_args = parts.join(" ");
            }
        }

        // If we found arguments, aggressively patch the label for Sprout
        if !extracted_args.is_empty() {
            let prefixes = ["LABEL=", "label=", "CDLABEL=", "archisolabel="];
            for prefix in prefixes {
                let mut current = 0;
                while let Some(idx) = extracted_args[current..].find(prefix) {
                    let start = current + idx + prefix.len();
                    // Find the end of the label string (space, quote, or end of line)
                    let end_offset = extracted_args[start..]
                        .find(|c: char| c == ' ' || c == '"' || c == '\'')
                        .unwrap_or(extracted_args[start..].len());
                    let end = start + end_offset;
                    
                    // Replace whatever garbage label they had with our strict 11-char FAT32 label
                    extracted_args = format!("{}{}{}", &extracted_args[..start], new_usb_label, &extracted_args[end..]);
                    current = start + new_usb_label.len();
                }
            }
            
            *found_args = Some(extracted_args);
            return;
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