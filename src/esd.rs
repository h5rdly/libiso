use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    fmt::Write,
    collections::HashMap
};

use pyo3::prelude::*;
use pyo3::types::PyDict;


pub struct WimInfo {
    pub architecture: Option<String>,
    pub editions: Vec<String>,
    pub total_size_bytes: u64,
    pub raw_xml: String, 
}


fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);
    if let Some(start) = xml.find(&start_tag) {
        if let Some(end) = xml[start..].find(&end_tag) {
            let val_start = start + start_tag.len();
            let val_end = start + end;
            return Some(xml[val_start..val_end].trim().to_string());
        }
    }
    None
}


#[pyclass]
#[derive(Debug)]
pub struct WimFileEntry {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub attributes: u32,
    #[pyo3(get)]
    pub file_hash: String, // Converted to hex string for easy Python usage
    #[pyo3(get)]
    pub is_dir: bool,
}

fn parse_dentry_tree_internal(
    data: &[u8],
    mut offset: usize,
    parent_path: &str,
    files: &mut Vec<WimFileEntry>
) {
    while offset + 8 <= data.len() {
        // Read length of current dentry
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&data[offset..offset+8]);
        let dentry_length = u64::from_le_bytes(len_bytes) as usize;

        // A length of 8 (or 0 padded to 8) means 'no more files in this folder'
        if dentry_length <= 8 {
            break;
        }

        // Bounds check the 102-byte fixed header
        if offset + 102 > data.len() {
            break;
        }

        let attributes = u32::from_le_bytes(data[offset+8..offset+12].try_into().unwrap());
        let subdir_offset = u64::from_le_bytes(data[offset+16..offset+24].try_into().unwrap()) as usize;
        
        // Extract 20-byte hash as a hex string
        let mut file_hash = String::with_capacity(40);
        for byte in &data[offset+64..offset+84] {
            write!(&mut file_hash, "{:02x}", byte).unwrap();
        }

        let name_nbytes = u16::from_le_bytes(data[offset+100..offset+102].try_into().unwrap()) as usize;
        let name_offset = offset + 102;
        
        // Extract and decode UTF-16LE filename
        let file_name = if name_nbytes > 0 && name_offset + name_nbytes <= data.len() {
            let name_slice = &data[name_offset..name_offset+name_nbytes];
            let chars: Vec<u16> = name_slice.chunks_exact(2)
                .map(|c| u16::from_le_bytes([c[0], c[1]]))
                .collect();
            String::from_utf16_lossy(&chars)
        } else {
            String::new()
        };

        // Construct the full path
        let full_path = if parent_path.is_empty() {
            file_name.clone()
        } else if file_name.is_empty() {
            parent_path.to_string()
        } else {
            format!("{}/{}", parent_path, file_name)
        };

        // Save the file info (skip empty root nodes)
        if !file_name.is_empty() {
            files.push(WimFileEntry {
                path: full_path.clone(),
                attributes,
                file_hash,
                is_dir: (attributes & 0x10) != 0, // FILE_ATTRIBUTE_DIRECTORY
            });
        }

        // Recurse into children
        if subdir_offset > 0 && subdir_offset < data.len() {
            parse_dentry_tree_internal(data, subdir_offset, &full_path, files);
        }

        // Dentries are aligned to 8-byte boundaries
        let aligned_length = (dentry_length + 7) & !7;
        offset += aligned_length;
    }
}


#[pyclass]
pub struct EsdArchive {
    file: File,
    solid_uncompressed_size: u64,
    solid_chunk_size: u32,
    chunk_offsets: Vec<u64>,
    data_start: u64,
    #[pyo3(get)]
    pub num_chunks: usize,
}

#[pymethods]
impl EsdArchive {
    #[new]
    pub fn new(file_path: &str) -> PyResult<Self> {
        let mut file = File::open(file_path)?;
        
        // Read the 152 byte WIM header
        let mut header = [0u8; 152];
        file.read_exact(&mut header)?;

        if &header[0..8] != b"MSWIM\x00\x00\x00" && &header[0..8] != b"WLPWM\x00\x00\x00" {
            return Err(pyo3::exceptions::PyValueError::new_err("Not a valid WIM/ESD file."));
        }

        // The Solid resource header is directly after
        let mut solid_hdr = [0u8; 16];
        file.read_exact(&mut solid_hdr)?;

        let solid_uncompressed_size = u64::from_le_bytes(solid_hdr[0..8].try_into().unwrap());
        let comp_format = u32::from_le_bytes(solid_hdr[8..12].try_into().unwrap());
        let solid_chunk_size = u32::from_le_bytes(solid_hdr[12..16].try_into().unwrap());

        if comp_format != 3 {
            return Err(pyo3::exceptions::PyValueError::new_err("Expected LZMS compression (3)"));
        }

        let num_chunks = ((solid_uncompressed_size + solid_chunk_size as u64 - 1) / solid_chunk_size as u64) as usize;

        // Read the array of u64 chunk offsets
        let mut chunk_offsets = vec![0u64; num_chunks];
        let mut offsets_buf = vec![0u8; num_chunks * 8];
        file.read_exact(&mut offsets_buf)?;

        for i in 0..num_chunks {
            chunk_offsets[i] = u64::from_le_bytes(offsets_buf[i*8..(i+1)*8].try_into().unwrap());
        }

        let data_start = file.stream_position()?;

        Ok(Self {
            file,
            solid_uncompressed_size,
            solid_chunk_size,
            chunk_offsets,
            data_start,
            num_chunks,
        })
    }

    /// Reads, decompresses, and parses a specific chunk directly into a file tree
    pub fn get_wim_file_tree(&mut self, chunk_index: usize) -> PyResult<Vec<WimFileEntry>> {
        if chunk_index >= self.num_chunks {
            return Err(pyo3::exceptions::PyIndexError::new_err("Chunk index out of bounds"));
        }

        // Calculate chunk size and location
        let chunk_offset = if chunk_index == 0 {
            self.data_start
        } else {
            self.data_start + self.chunk_offsets[chunk_index - 1]
        };

        let chunk_comp_size = if chunk_index == 0 {
            self.chunk_offsets[0]
        } else {
            self.chunk_offsets[chunk_index] - self.chunk_offsets[chunk_index - 1]
        };

        let uncompressed_size = if chunk_index == self.num_chunks - 1 {
            let rem = self.solid_uncompressed_size % (self.solid_chunk_size as u64);
            if rem == 0 { self.solid_chunk_size as usize } else { rem as usize }
        } else {
            self.solid_chunk_size as usize
        };

        // Read raw compressed bytes
        self.file.seek(SeekFrom::Start(chunk_offset))?;
        let mut comp_data = vec![0u8; chunk_comp_size as usize];
        self.file.read_exact(&mut comp_data)?;

        // Decompress natively
        let mut decompressor = crate::lzms::LzmsDecompressor::new(&comp_data)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Invalid LZMS block"))?;
        
        let mut uncompressed_data = vec![0u8; uncompressed_size];
        decompressor.decompress_block(&mut uncompressed_data)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

        // Apply x86 translation filter
        let mut last_target_usages = Box::new([0i32; 65536]);
        crate::lzms::lzms_x86_filter(&mut uncompressed_data, &mut last_target_usages, true);

        // Parse the filesystem tree natively
        let mut files = Vec::new();
        parse_dentry_tree_internal(&uncompressed_data, 0, "", &mut files);

        Ok(files)
    }

    
    // Parse the WIM XML metadata to extract Windows editions and architecture
    pub fn get_wim_info<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        
        let total_size = self.file.metadata()?.len();
        
        let info = parse_wim_xml(total_size, |buf, offset| {
            self.file.seek(SeekFrom::Start(offset)).is_ok() && self.file.read_exact(buf).is_ok()
        }).ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Failed to parse WIM XML metadata"))?;

        let dict = PyDict::new(py);
        if let Some(arch) = info.architecture {
            dict.set_item("architecture", arch)?;
        }
        dict.set_item("editions", info.editions)?;
        dict.set_item("total_size_bytes", info.total_size_bytes)?;
        dict.set_item("raw_xml", info.raw_xml)?; 

        Ok(dict)
    }
}


pub fn parse_xml_payload(xml_bytes: &[u8]) -> Option<WimInfo> {

    if xml_bytes.len() < 2 || xml_bytes[0] != 0xFF || xml_bytes[1] != 0xFE || xml_bytes.len() % 2 != 0 {
        return None;
    }
    
    let utf16_data = &xml_bytes[2..];
    let mut utf16_chars = Vec::with_capacity(utf16_data.len() / 2);
    for chunk in utf16_data.chunks_exact(2) {
        utf16_chars.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    let xml_string = String::from_utf16(&utf16_chars).ok()?;

    let mut architectures = HashMap::new();
    let mut editions = Vec::new();
    let mut total_bytes = 0u64;
    let mut start_pos = 0;

    while let Some(image_start) = xml_string[start_pos..].find("<IMAGE") {
        let absolute_start = start_pos + image_start;
        if let Some(image_end) = xml_string[absolute_start..].find("</IMAGE>") {
            let absolute_end = absolute_start + image_end + 8;
            let image_xml = &xml_string[absolute_start..absolute_end];

            if let Some(bytes_str) = extract_tag(image_xml, "TOTALBYTES") {
                if let Ok(bytes) = bytes_str.parse::<u64>() {
                    total_bytes += bytes;
                }
            }

            if let Some(arch_val) = extract_tag(image_xml, "ARCH") {
                let arch_str = match arch_val.as_str() {
                    "0" => "x86",
                    "9" => "x64",
                    "5" => "ARM",
                    "12" => "ARM64",
                    _ => "Unknown",
                };
                *architectures.entry(arch_str.to_string()).or_insert(0) += 1;
            }

            let mut name = extract_tag(image_xml, "NAME").unwrap_or_default().to_lowercase();
            if name.is_empty() {
                name = extract_tag(image_xml, "EDITIONID").unwrap_or_default().to_lowercase();
            }
            if name.is_empty() {
                name = extract_tag(image_xml, "DISPLAYNAME").unwrap_or_default().to_lowercase();
            }

            if name.contains("pro") && !editions.contains(&"Pro".to_string()) { editions.push("Pro".to_string()); } 
            else if name.contains("home") && !editions.contains(&"Home".to_string()) { editions.push("Home".to_string()); } 
            else if name.contains("enterprise") && !editions.contains(&"Enterprise".to_string()) { editions.push("Enterprise".to_string()); } 
            else if name.contains("education") && !editions.contains(&"Education".to_string()) { editions.push("Education".to_string()); } 
            else if name.contains("server") && !editions.contains(&"Server".to_string()) { editions.push("Server".to_string()); }

            start_pos = absolute_end;
        } else {
            break;
        }
    }

    let primary_arch = architectures.into_iter().max_by_key(|&(_, count)| count).map(|(arch, _)| arch);

    Some(WimInfo { 
        architecture: primary_arch, 
        editions, 
        total_size_bytes: total_bytes,
        raw_xml: xml_string 
    })
}



pub fn parse_wim_xml<F>(total_size: u64, mut read_chunk: F) -> Option<WimInfo> where
    F: FnMut(&mut [u8], u64) -> bool 
{

    if total_size < 204 {
        return None;
    }

    let mut header = [0u8; 204];
    if !read_chunk(&mut header, 0) {
        return None;
    }

    if &header[0..8] != b"MSWIM\x00\x00\x00" && &header[0..8] != b"WLPWM\x00\x00\x00" {
        return None;
    }

    let xml_res_offset = 72;
    let mut size_arr = [0u8; 8];
    size_arr[..7].copy_from_slice(&header[xml_res_offset..xml_res_offset + 7]);
    let xml_size = u64::from_le_bytes(size_arr);
    
    let mut offset_arr = [0u8; 8];
    offset_arr.copy_from_slice(&header[xml_res_offset + 8..xml_res_offset + 16]);
    let xml_offset = u64::from_le_bytes(offset_arr);

    if xml_size == 0 || xml_size > 50 * 1024 * 1024 || xml_offset + xml_size > total_size {
        return None;
    }

    let mut xml_bytes = vec![0u8; xml_size as usize];
    if !read_chunk(&mut xml_bytes, xml_offset) {
        return None;
    }

    parse_xml_payload(&xml_bytes)
}
