use std::io::{Read, Seek};

use hadris_iso::sync::IsoImage;

use pelite::pe64::{Pe, PeFile};
use sha1::Sha1;
use sha2::{Sha256, Digest};

use cms::content_info::ContentInfo;
use cms::signed_data::SignedData;
use der::{Decode, Encode};

use crate::dbx::{TRUSTED_MS_CA_THUMBPRINTS, DBX_HASHES};


pub fn read_file_from_iso<T: Read + Seek>(iso: &IsoImage<T>, path: &str, ) -> Option<Vec<u8>> {
    
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


pub fn inspect_secure_boot_bytes(efi_bytes: &[u8]) -> SecureBootStatus {
    let mut status = SecureBootStatus {
        has_efi_bootloader: true,
        is_signed: false,
        is_microsoft_signed: false,
        is_revoked: false,
        signature_size: 0,
    };

    let mut image_hasher = Sha256::new();
    image_hasher.update(&efi_bytes);
    let flat_hash: String = image_hasher.finalize().iter().map(|b| format!("{:02X}", b)).collect();
    
    if DBX_HASHES.binary_search(&flat_hash.as_str()).is_ok() {
        status.is_revoked = true;
    }

    let pe = match PeFile::from_bytes(&efi_bytes) {
        Ok(pe) => pe,
        Err(_) => return status,
    };

    if let Ok(security) = pe.security() {
        let cert_data = security.certificate_data();
        if !cert_data.is_empty() {
            status.is_signed = true;
            status.signature_size = cert_data.len();

            if let Ok(content_info) = ContentInfo::from_der(cert_data) {
                if let Ok(signed_data) = content_info.content.decode_as::<SignedData>() {
                    if let Some(certs) = signed_data.certificates {
                        for cert_choice in certs.0.iter() {
                            if let cms::cert::CertificateChoices::Certificate(cert) = cert_choice {
                                if let Ok(raw_cert) = cert.to_der() {
                                    let mut cert_hasher = Sha1::new();
                                    cert_hasher.update(&raw_cert);
                                    let thumbprint: String = cert_hasher.finalize()
                                        .iter()
                                        .map(|b| format!("{:02X}", b))
                                        .collect();
                                    
                                    if TRUSTED_MS_CA_THUMBPRINTS.contains(&thumbprint.as_str()) {
                                        status.is_microsoft_signed = true;
                                        break; 
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    status
}


pub fn inspect_secure_boot<T: Read + Seek>(iso: &IsoImage<T>) -> SecureBootStatus {

    let efi_bytes = match read_file_from_iso(iso, "EFI/BOOT/BOOTX64.EFI")
        .or_else(|| read_file_from_iso(iso, "BOOTX64.EFI")) 
    {
        Some(bytes) => bytes,
        None => return SecureBootStatus {
            has_efi_bootloader: false,
            is_signed: false,
            is_microsoft_signed: false,
            is_revoked: false,
            signature_size: 0,
        },
    };
    inspect_secure_boot_bytes(&efi_bytes)
}