use std::io::{Read, Seek};
use hadris_iso::sync::IsoImage;
use hadris_iso::read::DirEntry;
use pelite::pe64::{Pe, PeView};

/// Helper function to walk the ISO directory tree and find a specific file
pub fn find_file_in_iso<T: Read + Seek>(
    iso: &IsoImage<T>,
    path: &str,
) -> Option<DirEntry> {
    let mut current_dir = iso.root_dir().dir_ref();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    for (i, part) in parts.iter().enumerate() {
        let dir = iso.open_dir(current_dir);
        let mut found = None;

        for entry_res in dir.entries() {
            if let Ok(entry) = entry_res {
                // Ignore special "." and ".." entries
                if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" {
                    continue;
                }
                
                let name = entry.display_name().into_owned();
                // ISO names might have a version suffix like ";1"
                let clean_name = name.split(';').next().unwrap_or(&name);
                
                if clean_name.eq_ignore_ascii_case(part) {
                    found = Some(entry);
                    break;
                }
            }
        }

        let entry = found?;
        if i == parts.len() - 1 {
            return Some(entry); // Found the target file!
        } else if entry.is_directory() {
            current_dir = entry.as_dir_ref(iso).ok()?; // Dive deeper
        } else {
            return None; // Path expected a directory, but found a file
        }
    }
    None
}

/// Represents the status of the Secure Boot signature
pub struct SecureBootStatus {
    pub has_efi_bootloader: bool,
    pub is_signed: bool,
    pub signature_size: usize,
}

/// Rips the PKCS#7 Authenticode signature out of an EFI bootloader
pub fn inspect_secure_boot<T: Read + Seek>(iso: &IsoImage<T>) -> SecureBootStatus {
    let mut status = SecureBootStatus {
        has_efi_bootloader: false,
        is_signed: false,
        signature_size: 0,
    };

    // 1. Find the UEFI Bootloader
    let bootloader_entry = match find_file_in_iso(iso, "EFI/BOOT/BOOTX64.EFI") {
        Some(entry) => entry,
        None => return status,
    };

    status.has_efi_bootloader = true;

    // 2. Extract the .efi file bytes into RAM
    let efi_bytes = match iso.read_file(&bootloader_entry) {
        Ok(bytes) => bytes,
        Err(_) => return status,
    };

    // 3. Parse the PE headers using `pelite`
    let pe = match PeView::from_bytes(&efi_bytes) {
        Ok(pe) => pe,
        Err(_) => return status, // Not a valid PE file
    };

    // 4. Rip out the Security Directory!
    if let Ok(security) = pe.security() {
        let cert_data = security.certificate_data();
        if !cert_data.is_empty() {
            status.is_signed = true;
            status.signature_size = cert_data.len();
        }
    }

    status
}