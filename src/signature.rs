use std::io::{Read, Seek};
use hadris_iso::sync::IsoImage;
use pelite::pe64::{Pe, PeFile};
use x509_parser::prelude::*;
use sha2::{Sha256, Digest};


const TRUSTED_MS_CA_THUMBPRINTS: &[&str] = &[
    "AEFC5FBBBE055D8F8DAA585473499417AB5A5272", 
    "81AA6B3244C935BCE0D6628AF39827421E32497D",
    "A92902398E16C49778CD90F99E4F9AE17C55AF50",
    "13ADBF4309BD82709C8CD54F316ED522988A1BD4E", 
];

// const DBX_REVOCATION_JSON: &str = include_str!("../libiso/dbx_info_msft_latest.json");

pub fn read_file_from_iso<T: Read + Seek>(iso: &IsoImage<T>, path: &str,) -> Option<Vec<u8>> {
    
    let mut current_dir = iso.root_dir().dir_ref();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    for (i, part) in parts.iter().enumerate() {
        let dir = iso.open_dir(current_dir);
        let mut found_dir_ref = None;
        let mut file_bytes = None;
        let mut found = false;

        for entry_res in dir.entries() {
            if let Ok(entry) = entry_res {
                if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" { continue; }
                
                let name = entry.display_name().into_owned();
                let clean_name = name.split(';').next().unwrap_or(&name);
                
                if clean_name.eq_ignore_ascii_case(part) {
                    found = true;
                    if i == parts.len() - 1 {
                        if !entry.is_directory() {
                            file_bytes = iso.read_file(&entry).ok();
                        }
                    } else if entry.is_directory() {
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
    pub is_revoked: bool, 
    pub signature_size: usize,
}

pub fn inspect_secure_boot<T: Read + Seek>(iso: &IsoImage<T>) -> SecureBootStatus {

    let mut status = SecureBootStatus {
        has_efi_bootloader: false,
        is_signed: false,
        is_microsoft_signed: false,
        is_revoked: false,
        signature_size: 0,
    };

    let efi_bytes = match read_file_from_iso(iso, "EFI/BOOT/BOOTX64.EFI") {
        Some(bytes) => bytes,
        None => return status,
    };

    status.has_efi_bootloader = true;

    // Check flat hash against the JSON revocation list
    let mut image_hasher = Sha256::new();
    image_hasher.update(&efi_bytes);
    let flat_hash: String = image_hasher.finalize().iter().map(|b| format!("{:02X}", b)).collect();
    // if DBX_REVOCATION_JSON.to_uppercase().contains(&flat_hash) {
    //     status.is_revoked = true;
    // }

    let pe = match PeFile::from_bytes(&efi_bytes) {
        Ok(pe) => pe,
        Err(_) => return status,
    };

    // Extract Security Directory and verify Certificates
    if let Ok(security) = pe.security() {
        let cert_data = security.certificate_data();
        if !cert_data.is_empty() {
            status.is_signed = true;
            status.signature_size = cert_data.len();

            let mut offset = 0;
            while offset < cert_data.len() {
                if cert_data[offset] == 0x30 {
                    if let Ok((_, cert)) = X509Certificate::from_der(&cert_data[offset..]) {
                        
                        let mut cert_hasher = Sha256::new();
                        cert_hasher.update(cert.as_raw());
                        let thumbprint: String = cert_hasher.finalize().iter().map(|b| format!("{:02X}", b)).collect();
                        
                        if TRUSTED_MS_CA_THUMBPRINTS.contains(&thumbprint.as_str()) {
                            status.is_microsoft_signed = true;
                            break; 
                        }
                    }
                }
                offset += 1;
            }
        }
    }

    status
}