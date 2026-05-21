use std::{io::Write, fs::File, path::Path, cell::RefCell};

use flate2::write::GzDecoder;
use zstd::stream::write::Decoder as ZstdDecoder;

use pyo3::{
    prelude::*, 
    types::{PyDict}, 
    exceptions::{PyRuntimeError, PyIOError, PyValueError}
};

use crate::image_parser::{ImageReader, IsoReader, UdfReader};
use crate::{udf, esd};


#[derive(Debug, PartialEq)]
pub enum CompressionType {
    Gzip,
    Zstd,
    Xz,
    Uncompressed,
}


pub fn detect_compression(data: &[u8]) -> CompressionType {

    if data.len() >= 4 {
        match &data[0..4] {
            [0x1F, 0x8B, _, _] => return CompressionType::Gzip,
            [0x28, 0xB5, 0x2F, 0xFD] => return CompressionType::Zstd,
            [0xFD, 0x37, 0x7A, 0x58] => return CompressionType::Xz,
            _ => {}
        }
    }
    CompressionType::Uncompressed
}



fn extract_to_fs<R: ImageReader>(reader: &R, current_path: &str, host_dir: &Path, auto_decompress: bool) -> Result<(), String> {
    
    let entries = reader.list_dir(current_path)?;
    for entry in entries {
        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }

        let new_img_path = if current_path.is_empty() { format!("/{}", clean_name) } else { format!("{}/{}", current_path, clean_name) };
        let new_host_path = host_dir.join(&clean_name);

        if entry.is_dir {
            std::fs::create_dir_all(&new_host_path).map_err(|e| e.to_string())?;
            extract_to_fs(reader, &new_img_path, &new_host_path, auto_decompress)?;
        } else {
            let stream_path = entry.symlink_target.unwrap_or(new_img_path);
            let out_file = File::create(&new_host_path).map_err(|e| e.to_string())?;

            if !auto_decompress {
                let mut writer = out_file;
                reader.stream_file(&stream_path, &mut |chunk| {
                    writer.write_all(chunk).map_err(|e| e.to_string())
                })?;
                continue;
            }

            //  Buffer Interceptor 
            let mut writer_opt: Option<Box<dyn Write>> = None;
            let mut magic_buffer = Vec::new();
            let mut file_taken = Some(out_file); // Take ownership so we can pass it into the decoders

            reader.stream_file(&stream_path, &mut |chunk| {
                if writer_opt.is_none() {
                    magic_buffer.extend_from_slice(chunk);
                    
                    // Wait until we have 4 bytes to check magic headers (or EOF)
                    if magic_buffer.len() >= 4 || chunk.is_empty() {
                        let comp_type = detect_compression(&magic_buffer);
                        let f = file_taken.take().unwrap();
                        
                        // Dynamically swap the writing stream!
                        let mut w: Box<dyn Write> = match comp_type {
                            CompressionType::Gzip => Box::new(GzDecoder::new(f)),
                            CompressionType::Zstd => Box::new(ZstdDecoder::new(f).map_err(|e| e.to_string())?),
                            _ => Box::new(f), // Uncompressed fallback
                        };
                        
                        w.write_all(&magic_buffer).map_err(|e| e.to_string())?;
                        magic_buffer.clear();
                        writer_opt = Some(w);
                    }
                } else {
                    // We already wrapped it, just push the chunk!
                    writer_opt.as_mut().unwrap().write_all(chunk).map_err(|e| e.to_string())?;
                }
                Ok(())
            })?;

            // Finalize: If the file was smaller than 4 bytes, write whatever we buffered
            if writer_opt.is_none() {
                if let Some(mut f) = file_taken.take() {
                    f.write_all(&magic_buffer).map_err(|e| e.to_string())?;
                }
            } else {
                writer_opt.as_mut().unwrap().flush().map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}



#[pyfunction]
#[pyo3(signature = (image_path, extract_dir, auto_decompress=true))]
pub fn extract_image(image_path: String, extract_dir: String, auto_decompress: bool) -> PyResult<()> {
    let host_root = Path::new(&extract_dir);
    if !host_root.exists() {
        std::fs::create_dir_all(host_root)?;
    }

    let mut file = File::open(&image_path).map_err(|e| {
        PyIOError::new_err(format!("Failed to open ISO: {}", e))
    })?;

    let is_udf_valid = if let Ok(udf_ctx) = udf::mount_udf(&mut file) {
        udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
    } else { 
        false 
    };

    if is_udf_valid {
        let udf_ctx = udf::mount_udf(&mut file).unwrap();
        let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
        extract_to_fs(&reader, "", host_root, auto_decompress).map_err(|e| PyRuntimeError::new_err(e))?;
    } else {
        let root_dir = crate::iso9660::get_joliet_root_directory(&mut file)
            .unwrap_or(None)
            .unwrap_or_else(|| crate::iso9660::get_root_directory(&mut file).unwrap());
        let reader = IsoReader { file: RefCell::new(&mut file), root_dir };
        extract_to_fs(&reader, "", host_root, auto_decompress).map_err(|e| PyRuntimeError::new_err(e))?;
    }

    Ok(())
}


#[pyfunction]
#[pyo3(signature = (image_path, wim_path="sources/install.wim"))]
pub fn get_wim_info_from_iso<'py>(py: Python<'py>, image_path: String, wim_path: &str) -> PyResult<Bound<'py, PyDict>> {
    
    let wim_path_owned = wim_path.to_string();

    let wim_info_result: Option<esd::WimInfo> = py.detach(move || {
        
        let mut file = File::open(&image_path).ok()?;
        
        let is_udf_valid = if let Ok(udf_ctx) = udf::mount_udf(&mut file) {
            udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
        } else { false };

        let mut result = None;

        let mut execute_wim_scan = |reader: &dyn ImageReader| -> Result<(), String> {
            let mut current_offset = 0u64;
            let mut xml_offset = 0u64;
            let mut xml_size = 0u64;
            let mut xml_buffer = Vec::new();
            let mut header_buffer = Vec::new();

            let stream_res = reader.stream_file(&wim_path_owned, &mut |chunk| {
                if xml_size == 0 {
                    header_buffer.extend_from_slice(chunk);
                    if header_buffer.len() >= 204 {
                        let header = &header_buffer[0..204];
                        if &header[0..8] != b"MSWIM\x00\x00\x00" && &header[0..8] != b"WLPWM\x00\x00\x00" {
                            return Err("Invalid WIM Header".to_string());
                        }
                        
                        let mut size_arr = [0u8; 8];
                        size_arr[..7].copy_from_slice(&header[72..79]);
                        xml_size = u64::from_le_bytes(size_arr);
                        
                        let mut offset_arr = [0u8; 8];
                        offset_arr.copy_from_slice(&header[80..88]);
                        xml_offset = u64::from_le_bytes(offset_arr);
                        
                        if xml_size == 0 || xml_size > 50 * 1024 * 1024 {
                            return Err("Invalid XML bounds".to_string());
                        }
                    }
                }
                
                let chunk_end = current_offset + chunk.len() as u64;
                if xml_size > 0 && chunk_end > xml_offset {
                    let overlap_start = if current_offset < xml_offset { 
                        (xml_offset - current_offset) as usize 
                    } else { 0 };
                    
                    let overlap_data = &chunk[overlap_start..];
                    xml_buffer.extend_from_slice(overlap_data);
                    
                    if xml_buffer.len() as u64 >= xml_size {
                        xml_buffer.truncate(xml_size as usize);
                        result = esd::parse_xml_payload(&xml_buffer);
                        return Err("ABORT_SUCCESS".to_string());
                    }
                }

                current_offset += chunk.len() as u64;
                Ok(())
            });

            if let Err(e) = stream_res {
                if e != "ABORT_SUCCESS" { return Err(e); }
            }
            Ok(())
        };

        if is_udf_valid {
            let udf_ctx = udf::mount_udf(&mut file).unwrap();
            let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
            let _ = execute_wim_scan(&reader);
        } else {
            let root_dir = crate::iso9660::get_joliet_root_directory(&mut file).ok()?.unwrap_or_else(|| crate::iso9660::get_root_directory(&mut file).unwrap());
            let reader = IsoReader { file: RefCell::new(&mut file), root_dir };
            let _ = execute_wim_scan(&reader);
        }

        result
    });

    let info = wim_info_result.ok_or_else(|| PyValueError::new_err("Could not parse WIM info from ISO"))?;

    let dict = PyDict::new(py);
    if let Some(arch) = info.architecture {
        dict.set_item("architecture", arch)?;
    }
    dict.set_item("editions", info.editions)?;
    dict.set_item("total_size_bytes", info.total_size_bytes)?;
    dict.set_item("raw_xml", info.raw_xml)?;
    dict.set_item("suggested_label", info.suggested_label)?;

    Ok(dict.into_any().cast_into::<PyDict>().unwrap())
}

