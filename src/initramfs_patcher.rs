use std::{
    io::{Cursor, Read, Write, BufReader, Seek, SeekFrom, copy},
    path::Path, fs::File, collections::VecDeque,
};

use cpio::newc::{Builder, Reader, trailer};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use backhand::{FilesystemReader, InnerNode};

use pyo3::{prelude::*, exceptions::PyRuntimeError};

use crate::image_parser::ImageReader;
use crate::kmod::KmodIndex;


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


pub struct SquashfsReader<'a> {
    pub iso_file: &'a mut std::fs::File,
    pub squashfs_start_offset: u64, // Where the squashfs starts inside the ISO
    pub squashfs_size: u64,
    pub current_pos: u64,
}

impl<'a> Read for SquashfsReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {

        // Calculate how many bytes are left in the SquashFS partition
        let available = self.squashfs_size.saturating_sub(self.current_pos);
        if available == 0 { return Ok(0); }
        
        // Cap the read so we don't accidentally read past the SquashFS and into other ISO data
        let to_read = available.min(buf.len() as u64) as usize;
        
        // Perform the physical read on the host ISO file
        self.iso_file.seek(SeekFrom::Start(self.squashfs_start_offset + self.current_pos))?;
        let bytes_read = self.iso_file.read(&mut buf[..to_read])?;
        
        // Advance our virtual cursor
        self.current_pos += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl<'a> Seek for SquashfsReader<'a> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // Translate the local SquashFS seek into a global ISO seek!
        self.current_pos = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(n) => (self.current_pos as i64 + n) as u64,
            SeekFrom::End(n) => (self.squashfs_size as i64 + n) as u64,
        };
        
        self.iso_file.seek(SeekFrom::Start(self.squashfs_start_offset + self.current_pos))?;
        Ok(self.current_pos)
    }
}


pub fn locate_squashfs<R: ImageReader>(reader: &R) -> Result<(u64, u64, String), String> {
    
    let candidates = [
        "LiveOS/squashfs.img",        // Fedora, OpenMandriva, Mageia
        "casper/filesystem.squashfs", // Ubuntu, Mint, Pop!_OS
        "live/filesystem.squashfs",   // Debian Live
        "arch/x86_64/airootfs.sfs",   // Arch Linux
        "manjaro/x86_64/rootfs.sfs",  // Manjaro
        "boot/x86_64/rootfs.sfs",     // Other Arch derivatives
    ];

    for path in candidates {
        if let Ok((offset, size)) = reader.get_file_location(path) {
            return Ok((offset, size, path.to_string()));
        }
    }

    // BFS crawler fallback
    let mut queue = VecDeque::new();
    queue.push_back("".to_string());

    let mut largest_candidate = None;
    let mut max_size = 0;
    let min_valid_size = 100 * 1024 * 1024; // 100MB minimum

    while let Some(current_path) = queue.pop_front() {
        let entries = match reader.list_dir(&current_path) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for entry in entries {
            let clean_name = entry.name.as_str();
            if clean_name == "." || clean_name == ".." || clean_name.is_empty() {
                continue;
            }

            let full_path = if current_path.is_empty() {
                clean_name.to_string()
            } else {
                format!("{}/{}", current_path, clean_name)
            };

            if entry.is_dir {
                queue.push_back(full_path);
            } else {
                let name_lower = clean_name.to_lowercase();
                let is_squashfs_name = name_lower.contains("squashfs") 
                    || name_lower.ends_with(".sfs") 
                    || name_lower.ends_with("rootfs.img");

                if is_squashfs_name && entry.size > min_valid_size {
                    if entry.size > max_size {
                        max_size = entry.size;
                        largest_candidate = Some(full_path);
                    }
                }
            }
        }
    }

    if let Some(target_path) = largest_candidate {
        let (offset, size) = reader.get_file_location(&target_path)?;
        return Ok((offset, size, target_path));
    }

    Err("Crawled entire ISO: Could not locate a valid SquashFS root filesystem.".to_string())
}


// Converts a plain-text modules.dep string into a raw Kmod Binary Trie (.bin)
pub fn generate_modules_dep_bin(dep_text: &str) -> Vec<u8> {

    let mut index = KmodIndex::new();
    
    for line in dep_text.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        
        // Example line: "kernel/fs/fat/vfat.ko: kernel/fs/fat/fat.ko"
        if let Some((target, _deps)) = line.split_once(':') {
            // kmod routes the trie using the module basename without the .ko extension
            if let Some(modname) = Path::new(target.trim()).file_stem().and_then(|s| s.to_str()) {
                // Priority is 0. The value payload is the entire text line.
                index.insert(modname, line, 0);
            }
        }
    }
    
    let mut out = Cursor::new(Vec::new());
    index.write(&mut out).unwrap();
    out.into_inner()
}


fn inject_file(output_stream: &mut Cursor<Vec<u8>>, path: &str, data: &[u8]) -> Result<(), String> {
    
    let mut module_stream = Cursor::new(data);
    let builder = Builder::new(path).mode(0o100644).uid(0).gid(0).nlink(1);
    let mut file_writer = builder.write(output_stream, data.len() as u32);
    copy(&mut module_stream, &mut file_writer).map_err(|e| e.to_string())?;
    file_writer.finish().map_err(|e| e.to_string())?;
    
    Ok(())

}


pub fn extract_file_from_squashfs<R: Read + Seek + Send>(mut reader: R, target_filename: &str
) -> Result<(String, Vec<u8>), String> {
    
    reader.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;

    // Wrap whatever reader we got in a BufReader for speed
    let buf_reader = BufReader::new(reader);

    // Parse the filesystem tree
    let fs = FilesystemReader::from_reader(buf_reader).map_err(|e| e.to_string())?;

    let mut target_file_reader = None;
    let mut actual_filename = String::new();
    
    // Search the tree for the target file
    for node in fs.files() {
        if let Some(path_str) = node.fullpath.to_str() {
            if path_str.ends_with(target_filename) 
                || path_str.ends_with(&format!("{}.xz", target_filename))
                || path_str.ends_with(&format!("{}.zst", target_filename))
                || path_str.ends_with(&format!("{}.gz", target_filename)) 
            {
                if let InnerNode::File(squashfs_file) = &node.inner {
                    target_file_reader = Some(squashfs_file.clone());
                    if let Some(name) = node.fullpath.file_name().and_then(|n| n.to_str()) {
                        actual_filename = name.to_string();
                    }
                    break;
                }
            }
        }
    }

    let squashfs_file = target_file_reader.ok_or_else(|| format!("{} not found in SquashFS!", target_filename))?;
    
    let mut file_reader = fs.file(&squashfs_file).reader();
    let mut extracted_bytes = Vec::new();
    file_reader.read_to_end(&mut extracted_bytes).map_err(|e| e.to_string())?;

    Ok((actual_filename, extracted_bytes))
}


pub fn patch_initramfs<R: Read + Seek + Send>(
    raw_initramfs: &[u8], kernel_version: &str, squashfs_reader: &mut R, modules_to_fetch: &[&str],  
) -> Result<Vec<u8>, String> {
    
    let comp_type = detect_compression(raw_initramfs);
    println!("[*] Detected Initramfs compression: {:?}", comp_type);
    
    let uncompressed_cpio = match comp_type {
        CompressionType::Gzip => {
            let mut decoder = GzDecoder::new(raw_initramfs);
            let mut decoded = Vec::new();
            decoder.read_to_end(&mut decoded).map_err(|e| e.to_string())?;
            decoded
        },
        CompressionType::Zstd => zstd::decode_all(raw_initramfs).map_err(|e| e.to_string())?,
        CompressionType::Uncompressed => raw_initramfs.to_vec(),
        CompressionType::Xz => return Err("XZ decompression not implemented".to_string()),
    };

    let mut input_stream = Cursor::new(uncompressed_cpio);
    let mut output_stream = Cursor::new(Vec::new());
    let mut reader = Reader::new(&mut input_stream).map_err(|e| e.to_string())?;
    
    let mut old_dep_text = String::new();
    let mut dep_text_path = format!("usr/lib/modules/{}/modules.dep", kernel_version);

    // Stream through the CPIO, copying files, but intercepting the dependency files
    loop {
        if reader.entry().is_trailer() { break; }
        let file_name = reader.entry().name().to_string();
        
        // Drop the old binary files so we can regenerate them at the end
        if file_name.ends_with("modules.dep.bin") || file_name.ends_with("modules.alias.bin") || file_name.ends_with("modules.symbols.bin") {
            reader = Reader::new(reader.skip().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
            continue;
        }

        // Buffer the plain-text modules.dep so we can append to it
        if file_name.ends_with("modules.dep") {
            dep_text_path = file_name.clone();
            let mut buf = Vec::new();
            let temp_writer = reader.to_writer(Cursor::new(&mut buf)).map_err(|e| e.to_string())?;
            old_dep_text = String::from_utf8_lossy(&buf).to_string();
            
            // Advance the reader
            reader = Reader::new(temp_writer).map_err(|e| e.to_string())?;
            continue; // Don't write it yet! We'll write the patched version at the end.
        }
        
        // Intercept and patch the Mandriva Boot Script
        if file_name == "usr/bin/liveiso-root" || file_name == "sbin/liveiso-root" {
            // 1. EXTRACT METADATA FIRST (Before we consume the reader)
            let builder = Builder::new(&file_name)
                .ino(reader.entry().ino()).mode(reader.entry().mode()) 
                .uid(reader.entry().uid()).gid(reader.entry().gid())
                .nlink(reader.entry().nlink()).mtime(reader.entry().mtime())
                .dev_major(reader.entry().dev_major()).dev_minor(reader.entry().dev_minor())
                .rdev_major(reader.entry().rdev_major()).rdev_minor(reader.entry().rdev_minor());

            // 2. CONSUME THE READER TO GET THE DATA
            let mut buf = Vec::new();
            let temp_writer = reader.to_writer(Cursor::new(&mut buf)).map_err(|e| e.to_string())?;
            let mut script = String::from_utf8_lossy(&buf).to_string();

            // 3. INJECT OUR TRACER ROUNDS
            let injection = r#"
echo "[LIBISO] ========================================" > /dev/kmsg
echo "[LIBISO] STARTING LIVEISO-ROOT SCRIPT!" > /dev/kmsg
echo "[LIBISO] Target device: $livedev" > /dev/kmsg
echo "[LIBISO] UEFI flag: $liveuefi" > /dev/kmsg
echo "[LIBISO] ========================================" > /dev/kmsg
"#;
            script = script.replace("PATH=/usr/sbin:/usr/bin:/sbin:/bin", &format!("PATH=/usr/sbin:/usr/bin:/sbin:/bin\n{}", injection));

            // 4. WRITE IT ALL BACK
            let mut file_writer = builder.write(&mut output_stream, script.len() as u32);
            file_writer.write_all(script.as_bytes()).map_err(|e| e.to_string())?;
            file_writer.finish().map_err(|e| e.to_string())?;

            reader = Reader::new(temp_writer).map_err(|e| e.to_string())?;
            continue;
        }

        // Standard copy for everything else
        let builder = Builder::new(&file_name)
            .ino(reader.entry().ino()).mode(reader.entry().mode())
            .uid(reader.entry().uid()).gid(reader.entry().gid())
            .nlink(reader.entry().nlink()).mtime(reader.entry().mtime())
            .dev_major(reader.entry().dev_major()).dev_minor(reader.entry().dev_minor())
            .rdev_major(reader.entry().rdev_major()).rdev_minor(reader.entry().rdev_minor());

        let mut file_writer = builder.write(&mut output_stream, reader.entry().file_size());
        let next_stream = reader.to_writer(&mut file_writer).map_err(|e| e.to_string())?;
        file_writer.finish().map_err(|e| e.to_string())?;
        reader = Reader::new(next_stream).map_err(|e| e.to_string())?;
    }

    println!("[*] Extracting and Injecting payloads...");
    
    let base_mod_dir = format!("usr/lib/modules/{}/kernel/fs/fat", kernel_version);
    let nls_mod_dir = format!("usr/lib/modules/{}/kernel/fs/nls", kernel_version);

    let mut successfully_injected = Vec::new();

    for mod_name in modules_to_fetch {
        match extract_file_from_squashfs(&mut *squashfs_reader, mod_name) {
            Ok((_, mod_bytes)) => {
                let target_dir = if mod_name.starts_with("nls") { &nls_mod_dir } else { &base_mod_dir };
                let inject_path = format!("{}/{}", target_dir, mod_name);
                
                println!("    -> Injecting {}", inject_path);
                inject_file(&mut output_stream, &inject_path, &mod_bytes)?;
                successfully_injected.push(*mod_name);
            }
            Err(_) => {
                // If it's missing, it's likely built into vmlinuz directly
                println!("    -> Warning: {} not found in SquashFS. Skipping.", mod_name);
            }
        }
    }

    // Generate modules.dep 
    let mut appended_dep_text = old_dep_text.trim_end().to_string();
    appended_dep_text.push('\n');

    for mod_name in &successfully_injected {
        let target_dir = if mod_name.starts_with("nls") { &nls_mod_dir } else { &base_mod_dir };
        
        // vfat.ko depends on fat.ko (if we actually injected fat.ko)
        if *mod_name == "vfat.ko" && successfully_injected.contains(&"fat.ko") {
            appended_dep_text.push_str(&format!("{}/vfat.ko: {}/fat.ko\n", target_dir, base_mod_dir));
        } else {
            // Everything else has no dependencies
            appended_dep_text.push_str(&format!("{}/{}:\n", target_dir, mod_name));
        }
    }

    inject_file(&mut output_stream, &dep_text_path, appended_dep_text.as_bytes())?;

    // Generate .bin from scratch
    let generated_bin = generate_modules_dep_bin(&appended_dep_text);
    inject_file(&mut output_stream, &format!("{}.bin", dep_text_path), &generated_bin)?;

    // Seal the CPIO archive
    trailer(&mut output_stream).map_err(|e| e.to_string())?;
    let patched_cpio = output_stream.into_inner();

    println!("[*] Recompressing patched initramfs...");
    let final_bytes = match comp_type {
        CompressionType::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&patched_cpio).map_err(|e| e.to_string())?;
            encoder.finish().map_err(|e| e.to_string())?
        },
        CompressionType::Zstd => zstd::encode_all(Cursor::new(patched_cpio), 3).map_err(|e| e.to_string())?,
        _ => patched_cpio,
    };

    Ok(final_bytes)
}



#[pyfunction]
#[pyo3(name = "extract_file_from_squashfs")]
pub fn extract_file_from_squashfs_py(squashfs_path: &str, target_filename: &str
) -> PyResult<(String, Vec<u8>)> {
    
    let file = File::open(squashfs_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open SquashFS file: {}", e)))?;

    extract_file_from_squashfs(file, target_filename)
        .map_err(|e| PyRuntimeError::new_err(format!("SquashFS Extraction Failed: {}", e)))
}



#[pyfunction(name="patch_initramfs")]
#[pyo3(signature = (raw_initramfs, kernel_version, squashfs_path, modules_to_fetch))]
pub fn patch_initramfs_py(
    raw_initramfs: &[u8], kernel_version: &str, squashfs_path: &str, modules_to_fetch: Vec<String>
) -> PyResult<Vec<u8>> {
    
    let mut sq_file = File::open(squashfs_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open SquashFS: {}", e)))?;

    // Convert Vec<String> into Vec<&str>
    let modules_to_fetch: Vec<&str> = modules_to_fetch.iter().map(|s| s.as_str()).collect();

    patch_initramfs(
        raw_initramfs, kernel_version, &mut sq_file, &modules_to_fetch
    ).map_err(|e| PyRuntimeError::new_err(format!("Initramfs Patching Failed: {}", e)))
}