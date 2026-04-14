use std::io::{Read, Seek};
use hadris_iso::sync::IsoImage;
use pelite::pe64::{Pe, PeFile};
use x509_parser::prelude::*;


// walk the ISO directory tree, find a specific file, and return its bytes
pub fn read_file_from_iso<T: Read + Seek>(iso: &IsoImage<T>, path: &str,
) -> Option<Vec<u8>> {

    let mut current_dir = iso.root_dir().dir_ref();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    for (i, part) in parts.iter().enumerate() {
        let dir = iso.open_dir(current_dir);
        let mut found_dir_ref = None;
        let mut file_bytes = None;
        let mut found = false;

        for entry_res in dir.entries() {
            if let Ok(entry) = entry_res {
                if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" {
                    continue;
                }
                
                let name = entry.display_name().into_owned();
                let clean_name = name.split(';').next().unwrap_or(&name);
                
                if clean_name.eq_ignore_ascii_case(part) {
                    found = true;
                    if i == parts.len() - 1 {
                        // We reached the final part of the path, it should be the file!
                        if !entry.is_directory() {
                            file_bytes = iso.read_file(&entry).ok();
                        }
                    } else if entry.is_directory() {
                        // We are still traversing directories
                        found_dir_ref = entry.as_dir_ref(iso).ok();
                    }
                    break;
                }
            }
        }

        if !found { return None; }

        if i == parts.len() - 1 {
            return file_bytes;
        } else if let Some(dir_ref) = found_dir_ref {
            current_dir = dir_ref;
        } else {
            return None;
        }
    }
    None
}

pub struct SecureBootStatus {
    pub has_efi_bootloader: bool,
    pub is_signed: bool,
    pub is_microsoft_signed: bool,
    pub signature_size: usize,
}

// Rip the PKCS#7 Authenticode sig out of an EFI bootloader and find Microsoft certs
pub fn inspect_secure_boot<T: Read + Seek>(iso: &IsoImage<T>) -> SecureBootStatus {
    let mut status = SecureBootStatus {
        has_efi_bootloader: false,
        is_signed: false,
        is_microsoft_signed: false,
        signature_size: 0,
    };

    let efi_bytes = match read_file_from_iso(iso, "EFI/BOOT/BOOTX64.EFI") {
        Some(bytes) => bytes,
        None => return status,
    };

    status.has_efi_bootloader = true;

    let pe = match PeFile::from_bytes(&efi_bytes) {
        Ok(pe) => pe,
        Err(_) => return status,
    };

    // Rip out the Security directory
    if let Ok(security) = pe.security() {
        let cert_data = security.certificate_data();
        if !cert_data.is_empty() {
            status.is_signed = true;
            status.signature_size = cert_data.len();

            // Sliding window X.509 parser over the PKCS#7 blob
            let mut offset = 0;
            while offset < cert_data.len() {
                // 0x30 is the ASN.1 tag for SEQUENCE (which X.509 certs start with)
                if cert_data[offset] == 0x30 {
                    if let Ok((_, cert)) = X509Certificate::from_der(&cert_data[offset..]) {
                        let subject = cert.subject().to_string();
                        
                        // Check if this certificate belongs to the Microsoft UEFI CA
                        if subject.contains("Microsoft") && subject.contains("UEFI") {
                            status.is_microsoft_signed = true;
                            break; // We found the golden ticket, bail out!
                        }
                    }
                }
                offset += 1;
            }
        }
    }

    status
}