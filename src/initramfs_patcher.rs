use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path, fs::File, collections::VecDeque,  collections::HashSet,
};

use pyo3::{prelude::*, exceptions::PyRuntimeError};

use crate::image_parser::ImageReader;
use crate::kmod::KmodIndex;
use crate::squashfs::{
    read_superblock, read_root_inode, find_files, read_file_inode, extract_file_data, SQUASHFS_MAGIC
};


pub struct SquashfsReader<'a> {
    pub iso_file: &'a mut std::fs::File,
    pub squashfs_start_offset: u64,
    pub squashfs_size: u64,
    pub current_pos: u64,
}

impl<'a> Read for SquashfsReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let available = self.squashfs_size.saturating_sub(self.current_pos);
        if available == 0 { return Ok(0); }
        
        let to_read = available.min(buf.len() as u64) as usize;
        self.iso_file.seek(SeekFrom::Start(self.squashfs_start_offset + self.current_pos))?;
        let bytes_read = self.iso_file.read(&mut buf[..to_read])?;
        
        self.current_pos += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl<'a> Seek for SquashfsReader<'a> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
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

    let mut queue = VecDeque::new();
    queue.push_back("".to_string());

    let mut largest_candidate = None;
    let mut max_size = 0;
    let min_valid_size = 100 * 1024 * 1024; 

    while let Some(current_path) = queue.pop_front() {
        let entries = match reader.list_dir(&current_path) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for entry in entries {
            let clean_name = entry.name.as_str();
            if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }

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


pub fn generate_modules_dep_bin(dep_text: &str) -> Vec<u8> {
    let mut index = KmodIndex::new();
    for line in dep_text.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        if let Some((target, _deps)) = line.split_once(':') {
            if let Some(modname) = Path::new(target.trim()).file_stem().and_then(|s| s.to_str()) {
                index.insert(modname, line, 0);
            }
        }
    }
    let mut out = Cursor::new(Vec::new());
    index.write(&mut out).unwrap();
    out.into_inner()
}


fn write_cpio_header(out: &mut Vec<u8>, path: &str, filesize: u32) {

    let namesize = path.len() as u32 + 1; 
    let header = format!(
        "070701{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}{:08X}",
        0, 0x000081A4, 0, 0, 1, 0, filesize, 0, 0, 0, 0, namesize, 0
    );
    out.extend_from_slice(header.as_bytes());
}


pub fn patch_initramfs<R: Read + Seek + Send>(
    original_initrd: &[u8], kernel_version: &str, mut squash_reader: R, modules_to_fetch: &[&str],
) -> Result<Vec<u8>, String> {
    
    let mut final_initrd = original_initrd.to_vec();
    
    // The Linux kernel CPIO parser requires the magic header to be strictly 4-byte aligned
    while final_initrd.len() % 4 != 0 {
        final_initrd.push(0);
    }

    let mut appended_cpio = Vec::new();
    let kver_short = kernel_version.split_whitespace().next().unwrap_or(kernel_version);

    // 1. Map the disk with our bare-metal engine
    let superblock = read_superblock(&mut squash_reader).map_err(|e| e.to_string())?;
    let is_le = superblock.magic == SQUASHFS_MAGIC;

    // 2. Build the hit list
    let mut search_list = HashSet::new();
    for &mod_name in modules_to_fetch {
        search_list.insert(mod_name.to_string());
        search_list.insert(format!("{}.xz", mod_name));
        search_list.insert(format!("{}.zst", mod_name));
        search_list.insert(format!("{}.gz", mod_name));
    }

    // 3. Get the Root Directory
    let (_, root_loc) = read_root_inode(&mut squash_reader, &superblock, is_le)
        .map_err(|e| e.to_string())?;

    // 4. Sweep the entire OS tree for the drivers!
    // We pass kver_short so we only match drivers for the correct kernel version
    let found_files = find_files(
        &mut squash_reader, &superblock, root_loc, &mut search_list, kver_short, is_le, 
    ).map_err(|e| e.to_string())?;

    // 5. Rip each driver off the disk and append it to the CPIO
    for target in found_files {
        let blueprint = read_file_inode(
            &mut squash_reader, &superblock, target.block_index, target.block_offset, is_le
        ).map_err(|e| e.to_string())?;
        
        let mod_data = extract_file_data(&mut squash_reader, &superblock, &blueprint, is_le)
            .map_err(|e| e.to_string())?;

        let actual_path = target.path.trim_start_matches('/').to_string();
        println!("    -> Appending {} to initramfs...", actual_path);
        
        write_cpio_header(&mut appended_cpio, &actual_path, mod_data.len() as u32);
        appended_cpio.extend_from_slice(actual_path.as_bytes());
        appended_cpio.push(0);
        while appended_cpio.len() % 4 != 0 { appended_cpio.push(0); }
        
        appended_cpio.extend_from_slice(&mod_data);
        while appended_cpio.len() % 4 != 0 { appended_cpio.push(0); }
    }

    if !search_list.is_empty() {
        println!("    -> Warning: Could not find some modules in SquashFS: {:?}", search_list);
    }

    let trailer_path = "TRAILER!!!";
    write_cpio_header(&mut appended_cpio, trailer_path, 0);
    appended_cpio.extend_from_slice(trailer_path.as_bytes());
    appended_cpio.push(0);
    while appended_cpio.len() % 4 != 0 { appended_cpio.push(0); }

    final_initrd.extend(appended_cpio);

    Ok(final_initrd)
}



#[pyfunction(name="patch_initramfs")]
#[pyo3(signature = (raw_initramfs, kernel_version, squashfs_path, modules_to_fetch))]
pub fn patch_initramfs_py(
    raw_initramfs: &[u8], kernel_version: &str, squashfs_path: &str, modules_to_fetch: Vec<String>
) -> PyResult<Vec<u8>> {
    let mut sq_file = File::open(squashfs_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open SquashFS: {}", e)))?;

    let modules_to_fetch: Vec<&str> = modules_to_fetch.iter().map(|s| s.as_str()).collect();

    patch_initramfs(
        raw_initramfs, kernel_version, &mut sq_file, &modules_to_fetch
    ).map_err(|e| PyRuntimeError::new_err(format!("Initramfs Patching Failed: {}", e)))
}