use pelite::pe64::{Pe, PeFile};
use sha1::Sha1;
use sha2::{Sha256, Digest};

use cms::content_info::ContentInfo;
use cms::signed_data::SignedData;
use der::{Decode, Encode};

use crate::dbx::{TRUSTED_MS_CA_THUMBPRINTS, DBX_HASHES};



pub struct SecureBootStatus {
    pub is_signed: bool,
    pub is_microsoft_signed: bool,
    pub is_revoked: bool, 
    pub signature_size: usize,
}


pub fn inspect_secure_boot_bytes(efi_bytes: &[u8]) -> SecureBootStatus {
    let mut status = SecureBootStatus {
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


