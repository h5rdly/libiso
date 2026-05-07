use std::fs::File;
use std::io::Read;

use pelite::pe64::{Pe, PeFile};
use pelite::pattern as pat;

use pyo3::prelude::*;
use pyo3::exceptions::{PyIOError, PyValueError};



#[derive(PartialEq, Debug)]
pub enum GrubPatchStatus {
    Native,      // No CD-ROM prefixes found. It works out of the box (e.g., Mandriva)
    Patchable,   // Found prefixes, and we have enough room to patch them
    Unpatchable, // Found prefixes, but they are too big to safely overwrite (Needs Sprout)
    NotFound,    // Not GRUB (e.g. systemd-boot, rEFInd)
}


// Analyze an EFI binary to determine if and how it needs to be patched.
pub fn analyze_grub_efi(efi_bytes: &[u8], original_iso_label: &str, new_usb_label: &str) -> GrubPatchStatus {
    
    // 1. Is it GRUB at all?
    if !efi_bytes.windows(4).any(|w| w.eq_ignore_ascii_case(b"GRUB")) {
        return GrubPatchStatus::NotFound;
    } 

    // IF too small it's a CD-ROM stub - "bodyless" GRUB
    if efi_bytes.len() < 400_000 {
        return GrubPatchStatus::Unpatchable;
    }

    let nogo_patterns = vec![
        (format!(" '{}' ", original_iso_label), new_usb_label),
        (" '(cd0)/boot/grub' ".to_string(), "/boot/grub"),
        (" '(cd)/boot/grub' ".to_string(),  "/boot/grub"),
        (" '(cd0)/EFI/BOOT' ".to_string(),  "/EFI/BOOT"),
        (" '(cd)/EFI/BOOT' ".to_string(),   "/EFI/BOOT"),
    ];

    let mut found_any_matches = false;

    for (pat_str, replacement) in &nogo_patterns {
        if let Ok(offsets) = scan_efi_pattern(efi_bytes, pat_str) {
            for _ in offsets {
                found_any_matches = true;
                let old_len = pat_str.trim().trim_matches('\'').len();
                let new_len = replacement.as_bytes().len();

                if new_len > old_len {
                    return GrubPatchStatus::Unpatchable;
                }
            }
        }
    }

    if found_any_matches {
        GrubPatchStatus::Patchable
    } else {
        GrubPatchStatus::Native
    }
}



// Returns a list of physical file offsets where the pattern matched
pub fn scan_efi_pattern(efi_bytes: &[u8], pattern_str: &str) -> Result<Vec<usize>, String> {
    let pe = PeFile::from_bytes(efi_bytes).map_err(|_| "Not a valid 64-bit PE file")?;
    let pattern = pat::parse(pattern_str).map_err(|_| "Invalid pelite pattern syntax")?;
    
    let mut matches = pe.scanner().matches(&pattern, pe.headers().image_range());
    let mut save = [0u32; 1];
    let mut offsets = Vec::new();

    while matches.next(&mut save) {
        let rva = save[0];
        if let Ok(file_offset) = pe.rva_to_file_offset(rva) {
            offsets.push(file_offset);
        }
    }

    Ok(offsets)
}


/// Attempts to binary-patch a GRUB2 EFI payload.
/// Returns `true` if successfully patched, `false` if it must fallback to Sprout.
pub fn patch_grub_efi(efi_bytes: &mut [u8], original_iso_label: &str, new_usb_label: &str) -> bool {
    let mut patches_to_apply = Vec::new();

    let nogo_patterns = vec![
        (format!(" '{}' ", original_iso_label), new_usb_label),
        (" '(cd0)/boot/grub' ".to_string(), "/boot/grub"),
        (" '(cd)/boot/grub' ".to_string(),  "/boot/grub"),
        (" '(cd0)/EFI/BOOT' ".to_string(),  "/EFI/BOOT"),
        (" '(cd)/EFI/BOOT' ".to_string(),   "/EFI/BOOT"),
    ];

    for (pat_str, replacement) in &nogo_patterns {
        if let Ok(offsets) = scan_efi_pattern(efi_bytes, pat_str) {
            for file_offset in offsets {
                let old_len = pat_str.trim().trim_matches('\'').len(); // <--- Fixed length calc
                let new_bytes = replacement.as_bytes();

                if new_bytes.len() <= old_len {
                    patches_to_apply.push((file_offset, old_len, new_bytes));
                } else {
                    return false;
                }
            }
        }
    }

    let mut patched_anything = false;

    for (file_offset, old_len, new_bytes) in patches_to_apply {
        let end_offset = file_offset + new_bytes.len();
        efi_bytes[file_offset..end_offset].copy_from_slice(new_bytes);
        
        for i in end_offset..(file_offset + old_len) {
            efi_bytes[i] = 0x00;
        }
        patched_anything = true;
    }

    patched_anything
}


#[pyfunction(name="scan_efi_pattern")]
#[pyo3(signature = (efi_path, pattern_str))]
pub fn scan_efi_pattern_py(efi_path: String, pattern_str: String) -> PyResult<Vec<usize>> {
    let mut f = File::open(&efi_path).map_err(|e| PyIOError::new_err(e.to_string()))?;
    let mut efi_bytes = Vec::new();
    f.read_to_end(&mut efi_bytes).map_err(|e| PyIOError::new_err(e.to_string()))?;

    // Pipe the data directly into our core Rust engine
    match scan_efi_pattern(&efi_bytes, &pattern_str) {
        Ok(offsets) => Ok(offsets),
        Err(err_msg) => Err(PyValueError::new_err(err_msg)),
    }
}


pub fn patch_memdisk_uuid_to_label(efi_data: &mut [u8], new_label: &str) -> bool {

    let search_prefix = b"search.fs_uuid '";
    
    if let Some(pos) = efi_data.windows(search_prefix.len()).position(|w| w == search_prefix) {
        
        // The timestamp is 22 bytes, followed by a closing quote. Total = 39 bytes
        // Replace this 39-byte block with a padded label search
        
        let mut replacement = vec![b' '; 39];
        replacement[..17].copy_from_slice(b"search.fs_label '");
        
        let label_bytes = new_label.as_bytes();
        let label_len = std::cmp::min(11, label_bytes.len());
        replacement[17..17 + label_len].copy_from_slice(&label_bytes[..label_len]);
        
        replacement[17 + label_len] = b'\'';
        
        // Apply the 39-byte patch directly into the EFI binary's memory
        efi_data[pos..pos + 39].copy_from_slice(&replacement);
        return true;
    }
    false
}