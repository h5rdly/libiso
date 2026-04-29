use std::str::FromStr;
use std::time::{SystemTime, Duration};

use der::{Encode, Decode, EncodePem, Sequence};
use der::asn1::{UtcTime, SetOfVec, OctetString, Any, ObjectIdentifier};

use spki::AlgorithmIdentifierOwned;
use sha2::{Sha256, Digest};
use pelite::pe64::{Pe, PeFile};
use pelite::image::IMAGE_DIRECTORY_ENTRY_SECURITY;

use signature::{Signer, SignatureEncoding}; //
use cms::cert::CertificateChoices;
use cms::content_info::{ContentInfo, CmsVersion};
use cms::signed_data::{SignedData, SignerInfo, SignerIdentifier, EncapsulatedContentInfo, 
    SignerInfos, CertificateSet};
use x509_cert::Certificate;
use x509_cert::{
    attr::Attribute,
    der::DecodePem,
    name::Name,
    serial_number::SerialNumber,
    time::{Time, Validity},
};

use x509_cert::builder::{CertificateBuilder, Builder};
use x509_cert::builder::profile::cabf;

use rsa::{RsaPrivateKey, pkcs1v15::SigningKey};
use rsa::pkcs8::{EncodePrivateKey, DecodePrivateKey, LineEnding};
use rand;

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;


pub const SPC_INDIRECT_DATA_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.3.6.1.4.1.311.2.1.4");

// "1.3.6.1.4.1.311.2.1.15" - SpcPeImageData
const SPC_PE_IMAGE_DATA_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.3.6.1.4.1.311.2.1.15");
// "2.16.840.1.101.3.4.2.1" - SHA-256
const SHA256_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("2.16.840.1.101.3.4.2.1");

const CONTENT_TYPE_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.9.3");
const MESSAGE_DIGEST_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.9.4");
const PKCS7_SIGNED_DATA_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.7.2");
const SHA256_WITH_RSA_ENCRYPTION_OBJID: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.11");


#[derive(Clone, Debug, Eq, PartialEq, Sequence)]
pub struct SpcAttributeTypeAndOptionalValue {
    pub type_id: ObjectIdentifier,
    pub value: Any, 
}


#[derive(Clone, Debug, Eq, PartialEq, Sequence)]
pub struct DigestInfo {
    pub digest_algorithm: AlgorithmIdentifierOwned,
    pub digest: OctetString, 
}


#[derive(Clone, Debug, Eq, PartialEq, Sequence)]
pub struct SpcIndirectDataContent {
    pub data: SpcAttributeTypeAndOptionalValue,
    pub message_digest: DigestInfo,
}


pub fn build_spc_indirect_data(authenticode_hash: &[u8]) -> Result<Vec<u8>, der::Error> {

    // This is the hardcoded DER encoding of the Microsoft `SpcPeImageData` struct.
    // It's boilerplate that translates to an empty bitstring and the unicode string "<<<Obsolete>>>"
    let spc_pe_image_data_der: &[u8] = &[
        0x30, 0x23, // SEQUENCE, length 35
        0x03, 0x01, 0x00, // BIT STRING, length 1, 0 unused bits
        0xA0, 0x1E, // [0] EXPLICIT, length 30
        0x80, 0x1C, // [0] IMPLICIT (Choice 0), length 28
        // "<<<Obsolete>>>" string
        0x00, 0x3c, 0x00, 0x3c, 0x00, 0x3c, 0x00, 0x4f, 0x00, 0x62, 
        0x00, 0x73, 0x00, 0x6f, 0x00, 0x6c, 0x00, 0x65, 0x00, 0x74, 
        0x00, 0x65, 0x00, 0x3e, 0x00, 0x3e, 0x00, 0x3e
    ];

    let content = SpcIndirectDataContent {
        data: SpcAttributeTypeAndOptionalValue {
            type_id: SPC_PE_IMAGE_DATA_OBJID,
            value: Any::from_der(spc_pe_image_data_der)?,
        },
        message_digest: DigestInfo {
            digest_algorithm: AlgorithmIdentifierOwned {
                oid: SHA256_OBJID,
                // SHA-256 requires explicitly NULL parameters in ASN.1
                parameters: Some(Any::from_der(&[0x05, 0x00])?), 
            },
            digest: OctetString::new(authenticode_hash)?,
        },
    };

    // Serialize the whole struct to raw DER bytes!
    content.to_der()
}


pub fn calculate_authenticode_hash(pe_bytes: &[u8]) -> Result<Vec<u8>, String> {
    // Assumes a 64-bit PE32+ EFI binary - no BOOTIA32.EFI support

    let pe = PeFile::from_bytes(pe_bytes).map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();

    // Find the absolute byte offset of the NT Headers
    let dos_header = pe.dos_header();
    let nt_headers_offset = dos_header.e_lfanew as usize;
    
    // In a 64-bit PE file, the CheckSum field is exactly 64 bytes into the OptionalHeader
    // (NT Signature = 4 bytes, FileHeader = 20 bytes. Total offset = 88)
    let checksum_offset = nt_headers_offset + 24 + 64;
    
    // The Security Directory (Index 4) is 112 bytes into the OptionalHeader, plus 4 previous 8-byte directories
    // (Total offset = 112 + 32 = 144)
    let security_dir_offset = nt_headers_offset + 24 + 144;
    
    let size_of_headers = pe.nt_headers().OptionalHeader.SizeOfHeaders as usize;

    // Chunk 1: Start of file up to CheckSum 
    hasher.update(&pe_bytes[0..checksum_offset]);

    // Chunk 2: After CheckSum up to Security Directory 
    // skip the 4 bytes of the CheckSum
    hasher.update(&pe_bytes[checksum_offset + 4..security_dir_offset]);

    // Chunk 3: After Security Directory up to end of Headers 
    // We skip the 8 bytes (VirtualAddress and Size) of the Security Directory
    hasher.update(&pe_bytes[security_dir_offset + 8..size_of_headers]);

    // Chunk 4: The PE Sections 
    // Sections must be hashed in the exact order they appear physically on disk
    let mut sections = pe.section_headers().image().to_vec();
    sections.sort_by_key(|sec| sec.PointerToRawData);

    let mut last_pos = size_of_headers;

    for sec in sections {
        let start = sec.PointerToRawData as usize;
        let size = sec.SizeOfRawData as usize;
        
        if size == 0 { continue; }

        // Hash any padding bytes between sections
        if start > last_pos {
            hasher.update(&pe_bytes[last_pos..start]);
        }

        hasher.update(&pe_bytes[start..start + size]);
        last_pos = start + size;
    }

    // Chunk 5: Trailing data (excluding existing signatures) 
    // If the file already had a signature, we stop right before it
    let file_size = pe_bytes.len();
    let security_dir = pe.data_directory()[IMAGE_DIRECTORY_ENTRY_SECURITY as usize];
    let cert_table_start = security_dir.VirtualAddress as usize;
    
    let end_of_hash = if cert_table_start > 0 && cert_table_start < file_size {
        cert_table_start
    } else {
        file_size
    };

    if end_of_hash > last_pos {
        hasher.update(&pe_bytes[last_pos..end_of_hash]);
    }

    Ok(hasher.finalize().to_vec())
}



pub fn sign_efi(
    pe_bytes: &[u8],
    priv_key_pem: &str,
    cert_pem: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {

    // Calculate the Authenticode Hash & Build SpcIndirectData
    let auth_hash = calculate_authenticode_hash(pe_bytes)?;
    let spc_der = build_spc_indirect_data(&auth_hash)?;

    // Parse the Keys
    let cert = Certificate::from_pem(cert_pem)?;
    let priv_key = RsaPrivateKey::from_pkcs8_pem(priv_key_pem)?;

    // Build the Signed Attributes
    // Attribute 1: Content Type (SpcIndirectDataContent)
    let ct_attr = Attribute {
        oid: CONTENT_TYPE_OBJID,
        values: SetOfVec::try_from(vec![Any::from_der(&SPC_INDIRECT_DATA_OBJID.to_der()?)?])?,
    };

    // Attribute 2: Message Digest (SHA-256 of the spc_der bytes)
    let spc_hash = Sha256::digest(&spc_der);
    let md_attr = Attribute {
        oid: MESSAGE_DIGEST_OBJID,
        values: SetOfVec::try_from(vec![Any::from_der(&OctetString::new(spc_hash.as_slice())?.to_der()?)?])?,
    };

    let mut signed_attrs_set = SetOfVec::new();
    signed_attrs_set.insert(ct_attr)?;
    signed_attrs_set.insert(md_attr)?;
    let signed_attrs = signed_attrs_set; 

    // Calculate the RSA Signature over the Signed Attributes
    let signing_key = SigningKey::<Sha256>::new(priv_key);
    let signature = signing_key.sign(&signed_attrs.to_der()?);

    // Build the SignerInfo
    let signer_info = SignerInfo {
        version: CmsVersion::V1,
        sid: SignerIdentifier::IssuerAndSerialNumber(cms::cert::IssuerAndSerialNumber {
            issuer: cert.tbs_certificate().issuer().clone(),
            serial_number: cert.tbs_certificate().serial_number().clone(),
        }),
        digest_alg: AlgorithmIdentifierOwned { oid: SHA256_OBJID, parameters: Some(Any::from_der(&[0x05, 0x00])?) },
        signed_attrs: Some(signed_attrs),
        signature_algorithm: AlgorithmIdentifierOwned { oid: SHA256_WITH_RSA_ENCRYPTION_OBJID, parameters: Some(Any::from_der(&[0x05, 0x00])?) },
        signature: OctetString::new(signature.to_bytes())?,
        unsigned_attrs: None,
    };

    // Build the CMS SignedData Envelope
    let signed_data = SignedData {
        version: CmsVersion::V1,
        digest_algorithms: SetOfVec::try_from(vec![AlgorithmIdentifierOwned { 
            oid: SHA256_OBJID, parameters: Some(Any::from_der(&[0x05, 0x00])?) 
        }])?,
        encap_content_info: EncapsulatedContentInfo {
            econtent_type: SPC_INDIRECT_DATA_OBJID,
            econtent: Some(Any::from_der(&spc_der)?),
        },
        certificates: Some(CertificateSet(SetOfVec::try_from(vec![CertificateChoices::Certificate(cert)])?)),
        crls: None,
        // SignerInfos is a wrapper struct around SetOfVec
        signer_infos: SignerInfos(SetOfVec::try_from(vec![signer_info])?),
    };

    let content_info = ContentInfo {
        content_type: PKCS7_SIGNED_DATA_OBJID,
        content: Any::from_der(&signed_data.to_der()?)?,
    };

    let pkcs7_der = content_info.to_der()?;
    let padded_len = (pkcs7_der.len() + 7) & !7; // 8-byte alignment
    let mut win_cert = Vec::new();
    
    win_cert.extend_from_slice(&((padded_len + 8) as u32).to_le_bytes()); // dwLength
    win_cert.extend_from_slice(&0x0200u16.to_le_bytes()); // wRevision (2.0)
    win_cert.extend_from_slice(&0x0002u16.to_le_bytes()); // wCertificateType (PKCS7)
    win_cert.extend_from_slice(&pkcs7_der); // bCertificate
    win_cert.resize(win_cert.len() + (padded_len - pkcs7_der.len()), 0); // Padding

    // Append to PE and update headers
    let mut final_pe = pe_bytes.to_vec();
    let original_size = final_pe.len();
    final_pe.extend_from_slice(&win_cert);

    // Locate the Security Directory and Checksum
    let pe_parsed = PeFile::from_bytes(pe_bytes)?;
    let nt_offset = pe_parsed.dos_header().e_lfanew as usize;
    let checksum_offset = nt_offset + 24 + 64;
    let security_dir_offset = nt_offset + 24 + 144;

    // Overwrite the Security Directory to point to our new WIN_CERTIFICATE
    final_pe[security_dir_offset..security_dir_offset + 4].copy_from_slice(&(original_size as u32).to_le_bytes());
    final_pe[security_dir_offset + 4..security_dir_offset + 8].copy_from_slice(&(win_cert.len() as u32).to_le_bytes());

    // Recalculate and overwrite the PE Checksum
    let checksum = compute_pe_checksum(&final_pe, checksum_offset);
    final_pe[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());

    Ok(final_pe)
}


// Helper to manually calculate the PE Header Checksum
fn compute_pe_checksum(bytes: &[u8], checksum_offset: usize) -> u32 {
    let mut checksum = 0u64;
    let mut iter = bytes.chunks_exact(2);
    let mut pos = 0;
    
    for chunk in iter.by_ref() {
        // Skip the existing checksum field bytes during calculation
        if pos == checksum_offset || pos == checksum_offset + 2 {
            pos += 2;
            continue;
        }
        let word = u16::from_le_bytes([chunk[0], chunk[1]]) as u64;
        checksum += word;
        checksum = (checksum >> 16) + (checksum & 0xFFFF);
        pos += 2;
    }
    
    if let Some(rem) = iter.remainder().first() {
        checksum += *rem as u64;
        checksum = (checksum >> 16) + (checksum & 0xFFFF);
    }
    
    checksum = (checksum >> 16) + (checksum & 0xFFFF);
    checksum += bytes.len() as u64;
    checksum as u32
}


#[pyfunction]
#[pyo3(signature = (pe_bytes, priv_key_pem, cert_pem))]
pub fn sign_efi_binary(pe_bytes: &[u8], priv_key_pem: &str, cert_pem: &str) -> PyResult<Vec<u8>> {
    sign_efi(pe_bytes, priv_key_pem, cert_pem).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to sign EFI binary: {}", e))
    })
}


#[pyfunction]
#[pyo3(signature = (common_name="libiso Secure Boot Key"))]
pub fn generate_secure_boot_keys(common_name: &str) -> PyResult<(String, String)> {
    let mut rng = rand::rng(); 
    
    // - Generate RSA 2048 Key
    let priv_key = RsaPrivateKey::new(&mut rng, 2048).map_err(|e| {
        PyRuntimeError::new_err(format!("RSA key generation failed: {}", e))
    })?;
    let pub_key = rsa::RsaPublicKey::from(&priv_key);

    // - Build the Subject Name (Required for the Profile)
    let name_str = format!("CN={}", common_name);
    let name = Name::from_str(&name_str).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to parse distinguished name: {}", e))
    })?;

    // - Create a CAB Forum compliant Root Profile
    let profile = cabf::Root::new(false, name).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to create cert profile: {}", e))
    })?;

    // - Setup Serial Number and Validity
    let serial_number = SerialNumber::from(1u32);
    let now = SystemTime::now();
    let not_before = UtcTime::from_system_time(now).unwrap();
    let not_after = UtcTime::from_system_time(now + Duration::from_secs(3650 * 24 * 60 * 60)).unwrap();
    let validity = Validity::new(Time::UtcTime(not_before), Time::UtcTime(not_after));

    // - Get SubjectPublicKeyInfo
    let spki = spki::SubjectPublicKeyInfoOwned::from_key(&pub_key).map_err(|e| {
        PyRuntimeError::new_err(format!("SPKI creation failed: {}", e))
    })?;

    // - Initialize Builder and Sign
    let signer = SigningKey::<Sha256>::new(priv_key.clone());
    
    let builder = CertificateBuilder::new(
        profile,
        serial_number,
        validity,
        spki,
    ).map_err(|e| PyRuntimeError::new_err(format!("Builder init failed: {}", e)))?;

    // the build method is part of the Builder trait.
    // requires the signer and the signature type (for RSA, it's usually just inferred)
    let cert = builder.build(&signer).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to build/sign certificate: {}", e))
    })?;

    // - Serialize to PEM strings
    let priv_key_pem = priv_key.to_pkcs8_pem(LineEnding::LF).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to encode private key PEM: {}", e))
    })?.to_string();

    let cert_pem = cert.to_pem(LineEnding::LF).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to encode certificate PEM: {}", e))
    })?;

    Ok((priv_key_pem, cert_pem))
}