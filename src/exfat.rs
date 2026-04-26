use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::Mutex;
use std::collections::HashMap;

use crate::writer::UsbWriter;


pub struct BareExFat<T: Read + Write + Seek> {
    pub inner: Mutex<T>,
    bytes_per_cluster: u64,
    heap_offset: u64,
    bitmap_offset: u64,
    bitmap: Mutex<Vec<u8>>,
    cluster_count: u32,
    root_cluster: u32,
    dir_map: Mutex<HashMap<String, u32>>,
}


impl<T: Read + Write + Seek> BareExFat<T> {
    pub fn mount(mut inner: T) -> Result<Self, String> {
        let mut boot = [0u8; 512];
        inner.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
        inner.read_exact(&mut boot).map_err(|e| e.to_string())?;

        let bytes_per_sector = 1u64 << boot[108];
        let bytes_per_cluster = bytes_per_sector * (1u64 << boot[109]);
        let heap_offset = u32::from_le_bytes(boot[88..92].try_into().unwrap()) as u64 * bytes_per_sector;
        let cluster_count = u32::from_le_bytes(boot[92..96].try_into().unwrap());
        let root_cluster = u32::from_le_bytes(boot[96..100].try_into().unwrap());

        // Find the Bitmap in the Root Directory
        let root_offset = heap_offset + (root_cluster as u64 - 2) * bytes_per_cluster;
        inner.seek(SeekFrom::Start(root_offset)).unwrap();
        let mut root_data = vec![0u8; bytes_per_cluster as usize];
        inner.read_exact(&mut root_data).unwrap();

        let mut bitmap_cluster = 0;
        let mut bitmap_len = 0;
        for i in 0..(root_data.len() / 32) {
            let entry = &root_data[i * 32 .. i * 32 + 32];
            if entry[0] == 0x81 { // 0x81 = Allocation Bitmap
                bitmap_cluster = u32::from_le_bytes(entry[20..24].try_into().unwrap());
                bitmap_len = u64::from_le_bytes(entry[24..32].try_into().unwrap());
                break;
            }
        }

        // Load Bitmap into RAM
        let bitmap_offset = heap_offset + (bitmap_cluster as u64 - 2) * bytes_per_cluster;
        inner.seek(SeekFrom::Start(bitmap_offset)).unwrap();
        let mut bitmap = vec![0u8; bitmap_len as usize];
        inner.read_exact(&mut bitmap).unwrap();

        Ok(Self {
            inner: Mutex::new(inner),
            bytes_per_cluster,
            heap_offset,
            bitmap_offset,
            bitmap: Mutex::new(bitmap),
            cluster_count,
            root_cluster,
            dir_map: Mutex::new(HashMap::new()),
        })
    }

    fn alloc_clusters(&self, bytes: u64) -> Result<u32, String> {
        if bytes == 0 { return Ok(0); }
        let clusters_needed = (bytes + self.bytes_per_cluster - 1) / self.bytes_per_cluster;
        
        let mut bitmap = self.bitmap.lock().unwrap();
        let mut consecutive = 0;
        let mut start_cluster = 2;
        
        for i in 0..self.cluster_count {
            let byte_idx = (i / 8) as usize;
            let bit_idx = i % 8;
            if (bitmap[byte_idx] & (1 << bit_idx)) == 0 {
                if consecutive == 0 { start_cluster = i + 2; }
                consecutive += 1;
                if consecutive == clusters_needed as u32 {
                    // Mark as used
                    for j in 0..clusters_needed as u32 {
                        let c = start_cluster + j - 2;
                        bitmap[(c / 8) as usize] |= 1 << (c % 8);
                    }
                    // Flush bitmap to metal
                    let mut inner = self.inner.lock().unwrap();
                    inner.seek(SeekFrom::Start(self.bitmap_offset)).unwrap();
                    inner.write_all(&bitmap).unwrap();
                    return Ok(start_cluster);
                }
            } else {
                consecutive = 0;
            }
        }
        Err("ExFAT Drive Full or Fragmented!".to_string())
    }

    fn append_entries(&self, dir_cluster: u32, entries: &[[u8; 32]]) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        let mut cluster_data = vec![0u8; self.bytes_per_cluster as usize];
        let offset = self.heap_offset + (dir_cluster as u64 - 2) * self.bytes_per_cluster;
        
        inner.seek(SeekFrom::Start(offset)).unwrap();
        inner.read_exact(&mut cluster_data).unwrap();
        
        let mut consecutive_free = 0;
        let mut start_idx = 0;
        
        for i in 0..(self.bytes_per_cluster as usize / 32) {
            let b = cluster_data[i * 32];
            // 0x00 = Unused, 0xE5 = Deleted, 0x05 = Deleted File
            if b == 0x00 || b == 0xE5 || b == 0x05 {
                if consecutive_free == 0 { start_idx = i; }
                consecutive_free += 1;
                if consecutive_free == entries.len() {
                    for (j, entry) in entries.iter().enumerate() {
                        let dest = (start_idx + j) * 32;
                        cluster_data[dest..dest + 32].copy_from_slice(entry);
                    }
                    inner.seek(SeekFrom::Start(offset)).unwrap();
                    inner.write_all(&cluster_data).unwrap();
                    return Ok(());
                }
            } else {
                consecutive_free = 0;
            }
        }
        Err("Directory cluster full! Cannot allocate more files.".to_string())
    }
}


// ── exFAT sprcification utilities

fn split_path(path: &str) -> (&str, &str) {
    let path = path.trim_matches('/');
    match path.rfind('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => ("", path),
    }
}

fn calc_name_hash(name: &str) -> u16 {
    let mut hash = 0u16;
    for c in name.encode_utf16() {
        let upper = if c >= 0x61 && c <= 0x7A { c - 0x20 } else { c };
        hash = hash.rotate_right(1).wrapping_add((upper & 0xFF) as u16);
        hash = hash.rotate_right(1).wrapping_add((upper >> 8) as u16);
    }
    hash
}

fn calc_checksum(entries: &[[u8; 32]]) -> u16 {
    let mut sum = 0u16;
    for (i, entry) in entries.iter().enumerate() {
        for (j, &b) in entry.iter().enumerate() {
            if i == 0 && (j == 2 || j == 3) { continue; }
            sum = sum.rotate_right(1).wrapping_add(b as u16);
        }
    }
    sum
}

fn build_entry_set(name: &str, cluster: u32, size: u64, is_dir: bool) -> Vec<[u8; 32]> {
    let utf16: Vec<u16> = name.encode_utf16().collect();
    let name_entries = (utf16.len() + 14) / 15;
    let secondary_count = 1 + name_entries;
    
    let mut entries = Vec::new();
    let dos_time = [0x00, 0x00, 0x21, 0x00]; // Jan 1, 1980
    
    // 1. File Directory Entry (0x85)
    let mut fde = [0u8; 32];
    fde[0] = 0x85;
    fde[1] = secondary_count as u8;
    fde[4] = if is_dir { 0x10 } else { 0x20 }; 
    fde[8..12].copy_from_slice(&dos_time);  
    fde[12..16].copy_from_slice(&dos_time); 
    fde[16..20].copy_from_slice(&dos_time); 
    entries.push(fde);
    
    // 2. Stream Extension Entry (0xC0)
    let mut see = [0u8; 32];
    see[0] = 0xC0;
    see[1] = if size > 0 || cluster > 0 { 0x03 } else { 0x00 }; // 0x03 = AllocPossible | NoFatChain
    see[3] = utf16.len() as u8;
    see[4..6].copy_from_slice(&calc_name_hash(name).to_le_bytes());
    see[8..16].copy_from_slice(&size.to_le_bytes()); // ValidDataLength (Fixing the bug!)
    see[20..24].copy_from_slice(&cluster.to_le_bytes());
    see[24..32].copy_from_slice(&size.to_le_bytes()); // DataLength (Fixing the bug!)
    entries.push(see);
    
    // 3. File Name Entries (0xC1)
    for i in 0..name_entries {
        let mut ne = [0u8; 32];
        ne[0] = 0xC1;
        let start = i * 15;
        for j in 0..15 {
            if start + j < utf16.len() {
                let bytes = utf16[start + j].to_le_bytes();
                ne[2 + j * 2] = bytes[0];
                ne[3 + j * 2] = bytes[1];
            }
        }
        entries.push(ne);
    }
    
    let checksum = calc_checksum(&entries);
    entries[0][2..4].copy_from_slice(&checksum.to_le_bytes());
    entries
}

// ── Trait implementations 

impl<T: Read + Write + Seek> UsbWriter for BareExFat<T> {
    type FileWriter<'w> = BareFileWriter<'w, T> where Self: 'w;

    fn create_dir(&self, path: &str) -> Result<(), String> {
        let (parent_path, name) = split_path(path);
        let mut dir_map = self.dir_map.lock().unwrap();
        let parent_cluster = *dir_map.get(parent_path).unwrap_or(&self.root_cluster);
        
        let new_cluster = self.alloc_clusters(self.bytes_per_cluster)?;
        
        let offset = self.heap_offset + (new_cluster as u64 - 2) * self.bytes_per_cluster;
        {
            let mut inner = self.inner.lock().unwrap();
            inner.seek(SeekFrom::Start(offset)).unwrap();
            inner.write_all(&vec![0u8; self.bytes_per_cluster as usize]).unwrap();
        }
        
        let entries = build_entry_set(name, new_cluster, self.bytes_per_cluster, true);
        self.append_entries(parent_cluster, &entries)?;
        
        dir_map.insert(path.trim_matches('/').to_string(), new_cluster);
        Ok(())
    }

    fn open_file_writer<'w>(&'w self, path: &str, size: u64) -> Result<Self::FileWriter<'w>, String> {
        let (parent_path, name) = split_path(path);
        let dir_map = self.dir_map.lock().unwrap();
        let parent_cluster = *dir_map.get(parent_path).unwrap_or(&self.root_cluster);
        
        let file_cluster = self.alloc_clusters(size)?;
        let entries = build_entry_set(name, file_cluster, size, false);
        self.append_entries(parent_cluster, &entries)?;
        
        let start_offset = if file_cluster > 0 {
            self.heap_offset + (file_cluster as u64 - 2) * self.bytes_per_cluster
        } else {
            0
        };
        
        Ok(BareFileWriter { fs: self, start_offset, bytes_written: 0, max_size: size })
    }
}

pub struct BareFileWriter<'w, T: Read + Write + Seek> {
    fs: &'w BareExFat<T>,
    start_offset: u64,
    bytes_written: u64,
    max_size: u64,
}

impl<'w, T: Read + Write + Seek> Write for BareFileWriter<'w, T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() { return Ok(0); }
        let mut inner = self.fs.inner.lock().unwrap();
        inner.seek(SeekFrom::Start(self.start_offset + self.bytes_written))?;
        let to_write = std::cmp::min(buf.len() as u64, self.max_size - self.bytes_written) as usize;
        inner.write_all(&buf[..to_write])?;
        self.bytes_written += to_write as u64;
        Ok(to_write)
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}