use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::Mutex;
use std::collections::HashMap;

use crate::writer::UsbWriter;

pub struct BareExFat<T: Read + Write + Seek> {
    pub inner: Mutex<T>,
    pub bytes_per_cluster: u64,
    pub heap_offset: u64,
    pub bitmap_offset: u64,
    pub bitmap: Mutex<Vec<u8>>,
    pub cluster_count: u32,
    pub root_cluster: u32,
    pub dir_map: Mutex<HashMap<String, u32>>,
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


    // Scans the directory clusters to find a specific file path and returns (first_cluster, size)
    pub fn find_file(&self, path: &str) -> Result<(u32, u64), String> {
        let mut current_cluster = self.root_cluster;
        let parts: Vec<&str> = path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
        let mut inner = self.inner.lock().unwrap();

        for (i, part) in parts.iter().enumerate() {
            let is_last = i == parts.len() - 1;
            let offset = self.heap_offset + (current_cluster as u64 - 2) * self.bytes_per_cluster;
            
            // Read the directory cluster
            inner.seek(SeekFrom::Start(offset)).unwrap();
            let mut dir_data = vec![0u8; self.bytes_per_cluster as usize];
            inner.read_exact(&mut dir_data).unwrap();

            let mut found = false;
            let mut j = 0;
            while j < dir_data.len() / 32 {
                let entry_type = dir_data[j * 32];
                if entry_type == 0x00 { break; } // End of directory
                
                if entry_type == 0x85 { // File Directory Entry
                    let secondary_count = dir_data[j * 32 + 1] as usize;
                    let stream_ext = &dir_data[(j + 1) * 32 .. (j + 2) * 32];
                    
                    let first_cluster = u32::from_le_bytes(stream_ext[20..24].try_into().unwrap());
                    let data_length = u64::from_le_bytes(stream_ext[8..16].try_into().unwrap()); // ValidDataLength!
                    let name_len = stream_ext[3] as usize;

                    let mut name_utf16 = Vec::new();
                    for k in 0..(secondary_count - 1) {
                        let name_entry = &dir_data[(j + 2 + k) * 32 .. (j + 3 + k) * 32];
                        for char_idx in 0..15 {
                            if name_utf16.len() < name_len {
                                let c = u16::from_le_bytes(name_entry[2 + char_idx * 2 .. 4 + char_idx * 2].try_into().unwrap());
                                name_utf16.push(c);
                            }
                        }
                    }
                    
                    let name = String::from_utf16_lossy(&name_utf16);
                    if name.eq_ignore_ascii_case(part) {
                        if is_last {
                            return Ok((first_cluster, data_length));
                        } else {
                            current_cluster = first_cluster;
                            found = true;
                            break;
                        }
                    }
                    j += secondary_count; // skip the rest of this entry set
                }
                j += 1;
            }
            
            if !found { return Err(format!("Not found: {}", part)); }
        }
        Err("Invalid path".to_string())
    }

    // Walks the entire directory tree and returns a formatted list of all files and folders
    pub fn inspect_all(&self) -> Result<Vec<String>, String> {
        let mut found_files = Vec::new();
        self.walk_dir(self.root_cluster, "", &mut found_files)?;
        Ok(found_files)
    }

    // Recursive helper to walk directory clusters
    fn walk_dir(&self, cluster: u32, current_path: &str, found_files: &mut Vec<String>) -> Result<(), String> {
        let offset = self.heap_offset + (cluster as u64 - 2) * self.bytes_per_cluster;
        let mut dir_data = vec![0u8; self.bytes_per_cluster as usize];
        
        // Scope the lock so we drop it before recursing!
        {
            let mut inner = self.inner.lock().unwrap();
            inner.seek(SeekFrom::Start(offset)).unwrap();
            inner.read_exact(&mut dir_data).unwrap();
        }

        let mut j = 0;
        while j < dir_data.len() / 32 {
            let entry_type = dir_data[j * 32];
            if entry_type == 0x00 { break; } // End of directory
            
            if entry_type == 0x85 { // File Directory Entry
                let secondary_count = dir_data[j * 32 + 1] as usize;
                
                // Read File Attributes (Offset 4, 2 bytes) -> 0x0010 is Directory
                let file_attrs = u16::from_le_bytes(dir_data[j * 32 + 4 .. j * 32 + 6].try_into().unwrap());
                let is_dir = (file_attrs & 0x0010) != 0;

                let stream_ext = &dir_data[(j + 1) * 32 .. (j + 2) * 32];
                let first_cluster = u32::from_le_bytes(stream_ext[20..24].try_into().unwrap());
                let name_len = stream_ext[3] as usize;

                let mut name_utf16 = Vec::new();
                for k in 0..(secondary_count - 1) {
                    let name_entry = &dir_data[(j + 2 + k) * 32 .. (j + 3 + k) * 32];
                    for char_idx in 0..15 {
                        if name_utf16.len() < name_len {
                            let c = u16::from_le_bytes(name_entry[2 + char_idx * 2 .. 4 + char_idx * 2].try_into().unwrap());
                            name_utf16.push(c);
                        }
                    }
                }
                
                let name = String::from_utf16_lossy(&name_utf16);
                if name != "." && name != ".." && !name.is_empty() {
                    let full_path = if current_path.is_empty() { name.clone() } else { format!("{}/{}", current_path, name) };
                    
                    if is_dir {
                        found_files.push(format!("[exFAT DIR ] {}", full_path));
                        self.walk_dir(first_cluster, &full_path, found_files)?;
                    } else {
                        found_files.push(format!("[exFAT FILE] {}", full_path));
                    }
                }
                j += secondary_count; // Skip the secondary entries
            }
            j += 1;
        }
        Ok(())
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
    
    // Stream Extension Entry (0xC0)
    let mut see = [0u8; 32];
    see[0] = 0xC0;
    see[1] = if size > 0 || cluster > 0 { 0x03 } else { 0x00 }; // 0x03 = AllocPossible | NoFatChain
    see[3] = utf16.len() as u8;
    see[4..6].copy_from_slice(&calc_name_hash(name).to_le_bytes());
    see[8..16].copy_from_slice(&size.to_le_bytes()); // ValidDataLength (Fixing the bug!)
    see[20..24].copy_from_slice(&cluster.to_le_bytes());
    see[24..32].copy_from_slice(&size.to_le_bytes()); // DataLength (Fixing the bug!)
    entries.push(see);
    
    // File Name Entries (0xC1)
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



// ── exFAT formatter

pub fn format_exfat<T: Read + Write + Seek>(
    drive: &mut T,
    volume_size: u64,
    volume_label: &str,
) -> Result<(), String> {
    let bytes_per_sector: u64 = 512;
    let vol_sectors = volume_size / bytes_per_sector;

    // Calculate cluster geometry (32KB for standard drives, 128KB for >32GB)
    let mb = 1024 * 1024u64;
    let bytes_per_cluster = if volume_size < 256 * mb { 4096 }
        else if volume_size < 32 * 1024 * mb { 32768 }
        else { 131072 };
    let sectors_per_cluster = bytes_per_cluster / bytes_per_sector;

    let fat_offset = 24u64; // Starts right after Boot Region (12) + Backup Boot Region (12)
    let mut fat_length = 1u64;
    let mut heap_offset = fat_offset + fat_length;
    let mut cluster_count = (vol_sectors - heap_offset) / sectors_per_cluster;

    // Iteratively resolve exact FAT size
    loop {
        let needed_fat = ((cluster_count + 2) * 4).div_ceil(bytes_per_sector);
        if needed_fat <= fat_length { break; }
        fat_length = needed_fat;
        heap_offset = fat_offset + fat_length;
        cluster_count = (vol_sectors - heap_offset) / sectors_per_cluster;
    }

    let serial = 0x12345678u32; // Standard dummy serial

    // The Minimal Compressed Upcase Table
    let mut upcase_data = Vec::new();
    upcase_data.extend_from_slice(&0xFFFFu16.to_le_bytes()); upcase_data.extend_from_slice(&97u16.to_le_bytes());
    for i in 0x0041u16..=0x005A { upcase_data.extend_from_slice(&i.to_le_bytes()); }
    upcase_data.extend_from_slice(&0xFFFFu16.to_le_bytes()); upcase_data.extend_from_slice(&101u16.to_le_bytes());
    for i in 0x00C0u16..=0x00D6 { upcase_data.extend_from_slice(&i.to_le_bytes()); }
    upcase_data.extend_from_slice(&0x00F7u16.to_le_bytes());
    for i in 0x00D8u16..=0x00DE { upcase_data.extend_from_slice(&i.to_le_bytes()); }
    upcase_data.extend_from_slice(&0x0178u16.to_le_bytes());
    upcase_data.extend_from_slice(&0xFFFFu16.to_le_bytes()); upcase_data.extend_from_slice(&32768u16.to_le_bytes());
    upcase_data.extend_from_slice(&0xFFFFu16.to_le_bytes()); upcase_data.extend_from_slice(&32512u16.to_le_bytes());
    
    let mut upcase_checksum = 0u32;
    for &b in &upcase_data { upcase_checksum = upcase_checksum.rotate_right(1).wrapping_add(b as u32); }

    let upcase_clusters = (upcase_data.len() as u64).div_ceil(bytes_per_cluster) as u32;
    let bitmap_bytes = cluster_count.div_ceil(8) as u64;
    let bitmap_clusters = bitmap_bytes.div_ceil(bytes_per_cluster) as u32;

    let bitmap_cluster = 2u32;
    let upcase_cluster = bitmap_cluster + bitmap_clusters;
    let root_cluster = upcase_cluster + upcase_clusters;

    // Construct Boot Region
    let mut boot = vec![0u8; 512];
    boot[0..3].copy_from_slice(&[0xEB, 0x76, 0x90]);
    boot[3..11].copy_from_slice(b"EXFAT   ");
    boot[72..80].copy_from_slice(&vol_sectors.to_le_bytes());
    boot[80..84].copy_from_slice(&(fat_offset as u32).to_le_bytes());
    boot[84..88].copy_from_slice(&(fat_length as u32).to_le_bytes());
    boot[88..92].copy_from_slice(&(heap_offset as u32).to_le_bytes());
    boot[92..96].copy_from_slice(&(cluster_count as u32).to_le_bytes());
    boot[96..100].copy_from_slice(&root_cluster.to_le_bytes());
    boot[100..104].copy_from_slice(&serial.to_le_bytes());
    boot[104..106].copy_from_slice(&0x0100u16.to_le_bytes()); // Rev 1.0
    boot[108] = bytes_per_sector.trailing_zeros() as u8;
    boot[109] = sectors_per_cluster.trailing_zeros() as u8;
    boot[110] = 1; // 1 FAT Table
    boot[111] = 0x80; // Drive select
    boot[112] = 0xFF; // Percent in use
    boot[510] = 0x55;
    boot[511] = 0xAA;

    let mut boot_region = vec![0u8; 12 * 512];
    boot_region[0..512].copy_from_slice(&boot);
    for i in 1..=8 {
        boot_region[i*512 + 510] = 0x55;
        boot_region[i*512 + 511] = 0xAA;
    }
    
    // Boot Checksum calculation
    let mut checksum = 0u32;
    for i in 0..11*512 {
        if i == 106 || i == 107 || i == 112 { continue; } // Skip flags
        checksum = checksum.rotate_right(1).wrapping_add(boot_region[i] as u32);
    }
    for i in 0..128 {
        boot_region[11*512 + i*4 .. 11*512 + i*4 + 4].copy_from_slice(&checksum.to_le_bytes());
    }

    drive.seek(SeekFrom::Start(0)).unwrap();
    drive.write_all(&boot_region).unwrap(); // Main Boot Region
    drive.write_all(&boot_region).unwrap(); // Backup Boot Region

    // Write FAT Table
    let fat_byte_offset = fat_offset * bytes_per_sector;
    drive.seek(SeekFrom::Start(fat_byte_offset)).unwrap();
    let fat_zeros = vec![0u8; (fat_length * bytes_per_sector) as usize];
    drive.write_all(&fat_zeros).unwrap(); // Zero the table
    
    drive.seek(SeekFrom::Start(fat_byte_offset)).unwrap();
    drive.write_all(&0xFFFFFFF8u32.to_le_bytes()).unwrap(); // Media type
    drive.write_all(&0xFFFFFFFFu32.to_le_bytes()).unwrap(); // Reserved 1
    
    // Link Bitmap Clusters
    for i in 0..bitmap_clusters-1 { drive.write_all(&(bitmap_cluster + i + 1).to_le_bytes()).unwrap(); }
    drive.write_all(&0xFFFFFFFFu32.to_le_bytes()).unwrap();
    // Link Upcase Clusters
    for i in 0..upcase_clusters-1 { drive.write_all(&(upcase_cluster + i + 1).to_le_bytes()).unwrap(); }
    drive.write_all(&0xFFFFFFFFu32.to_le_bytes()).unwrap();
    // Link Root Directory (1 cluster)
    drive.write_all(&0xFFFFFFFFu32.to_le_bytes()).unwrap();

    // 5. Write Allocation Bitmap
    let mut bitmap = vec![0u8; bitmap_bytes as usize];
    for c in 2..=root_cluster {
        let idx = (c - 2) as usize;
        bitmap[idx / 8] |= 1 << (idx % 8);
    }
    let heap_byte_offset = heap_offset * bytes_per_sector;
    drive.seek(SeekFrom::Start(heap_byte_offset)).unwrap();
    drive.write_all(&bitmap).unwrap();

    // Write Upcase Table
    let upcase_byte_offset = heap_byte_offset + (upcase_cluster as u64 - 2) * bytes_per_cluster;
    drive.seek(SeekFrom::Start(upcase_byte_offset)).unwrap();
    drive.write_all(&upcase_data).unwrap();

    // Write Root Directory
    let root_byte_offset = heap_byte_offset + (root_cluster as u64 - 2) * bytes_per_cluster;
    drive.seek(SeekFrom::Start(root_byte_offset)).unwrap();
    let mut root_data = vec![0u8; bytes_per_cluster as usize];
    
    // Vol Label Entry (0x83)
    root_data[0] = 0x83;
    let label_utf16: Vec<u16> = volume_label.encode_utf16().take(11).collect();
    root_data[1] = label_utf16.len() as u8;
    for (i, &c) in label_utf16.iter().enumerate() {
        root_data[2 + i*2 .. 4 + i*2].copy_from_slice(&c.to_le_bytes());
    }
    
    // Bitmap Entry (0x81)
    root_data[32] = 0x81;
    root_data[52..56].copy_from_slice(&bitmap_cluster.to_le_bytes());
    root_data[56..64].copy_from_slice(&bitmap_bytes.to_le_bytes());
    
    // Upcase Entry (0x82)
    root_data[64] = 0x82;
    root_data[68..72].copy_from_slice(&upcase_checksum.to_le_bytes());
    root_data[84..88].copy_from_slice(&upcase_cluster.to_le_bytes());
    root_data[88..96].copy_from_slice(&(upcase_data.len() as u64).to_le_bytes());

    drive.write_all(&root_data).unwrap();
    drive.flush().unwrap();
    
    Ok(())
}