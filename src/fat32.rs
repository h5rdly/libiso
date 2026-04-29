use std::{
    io::{Read, Write, Seek, SeekFrom}, 
    time::{SystemTime, UNIX_EPOCH},
    sync::Mutex, 
    collections::HashMap,
};

pub fn format_fat32<T: Write + Seek>(
    drive: &mut T, volume_size: u64, volume_label: &str, start_lba: u32,
) -> Result<(), String> {

    let bytes_per_sector = 512u64;
    let total_sectors = (volume_size / bytes_per_sector) as u32;

    if total_sectors < 65525 {
        return Err("Drive too small for FAT32 (must be > ~32MB)".to_string());
    }

    // Calculate Microsoft's standard Sectors Per Cluster based on drive size
    // must guarantee cluster_count >= 65525 or it is legally FAT16
    let sectors_per_cluster: u32 = if total_sectors <= 65525 * 2 { 1 } // < 64MB: 512B
    else if total_sectors <= 65525 * 4 { 2 } // < 128MB: 1KB
    else if total_sectors <= 65525 * 8 { 4 } // < 256MB: 2KB
    else if total_sectors <= 16777216 { 8 }  // < 8GB:   4KB
    else if total_sectors <= 33554432 { 16 } // < 16GB:  8KB
    else if total_sectors <= 67108864 { 32 } // < 32GB:  16KB
    else { 64 };                             // > 32GB:  32KB

    let reserved_sectors = 32u32;
    let num_fats = 2u32;

    // Iteratively calculate exact FAT size
    let mut sectors_per_fat = 1u32;
    let mut cluster_count;
    loop {
        let data_sectors = total_sectors
            .saturating_sub(reserved_sectors)
            .saturating_sub(num_fats * sectors_per_fat);
        
        cluster_count = data_sectors / sectors_per_cluster;
        
        let needed_fat_bytes = (cluster_count + 2) * 4;
        let needed_fat_sectors = needed_fat_bytes.div_ceil(bytes_per_sector as u32);
        
        if needed_fat_sectors <= sectors_per_fat { break; }
        sectors_per_fat = needed_fat_sectors;
    }

    let root_cluster = 2u32;
    
    let sys_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let vol_id = (sys_time & 0xFFFFFFFF) as u32;

    // Construct Boot Sector (BPB)
    let mut boot = vec![0u8; 512];
    boot[0..3].copy_from_slice(&[0xEB, 0x58, 0x90]); // JMP instruction
    boot[3..11].copy_from_slice(b"MSWIN4.1");
    boot[11..13].copy_from_slice(&(bytes_per_sector as u16).to_le_bytes());
    boot[13] = sectors_per_cluster as u8;
    boot[14..16].copy_from_slice(&(reserved_sectors as u16).to_le_bytes());
    boot[16] = num_fats as u8;
    boot[17..19].copy_from_slice(&0u16.to_le_bytes()); // Root entries (0 for FAT32)
    boot[19..21].copy_from_slice(&0u16.to_le_bytes()); // Total sectors 16 (0 for FAT32)
    boot[21] = 0xF8; // Media type (Fixed disk)
    boot[22..24].copy_from_slice(&0u16.to_le_bytes()); // FAT16 size (0 for FAT32)
    boot[24..26].copy_from_slice(&63u16.to_le_bytes()); // Sectors per track
    boot[26..28].copy_from_slice(&255u16.to_le_bytes()); // Heads
    boot[28..32].copy_from_slice(&start_lba.to_le_bytes()); // Hidden sectors
    boot[32..36].copy_from_slice(&total_sectors.to_le_bytes()); // Total sectors 32
    
    // FAT32 Extended block
    boot[36..40].copy_from_slice(&sectors_per_fat.to_le_bytes());
    boot[40..42].copy_from_slice(&0u16.to_le_bytes()); // Flags
    boot[42..44].copy_from_slice(&0u16.to_le_bytes()); // Version
    boot[44..48].copy_from_slice(&root_cluster.to_le_bytes());
    boot[48..50].copy_from_slice(&1u16.to_le_bytes()); // FSInfo sector
    boot[50..52].copy_from_slice(&6u16.to_le_bytes()); // Backup boot sector
    boot[64] = 0x80; // Drive number
    boot[66] = 0x29; // Extended signature
    boot[67..71].copy_from_slice(&vol_id.to_le_bytes());
    
    // Volume Label (11 bytes, space padded)
    let mut label_bytes = [b' '; 11];
    for (i, c) in volume_label.chars().take(11).enumerate() {
        label_bytes[i] = c.to_ascii_uppercase() as u8;
    }
    boot[71..82].copy_from_slice(&label_bytes);
    boot[82..90].copy_from_slice(b"FAT32   ");
    boot[510] = 0x55;
    boot[511] = 0xAA;

    // Construct FSInfo Sector
    let mut fsinfo = vec![0u8; 512];
    fsinfo[0..4].copy_from_slice(&0x41615252u32.to_le_bytes()); // Lead sig
    fsinfo[484..488].copy_from_slice(&0x61417272u32.to_le_bytes()); // Struc sig
    fsinfo[488..492].copy_from_slice(&(cluster_count - 1).to_le_bytes()); // Free count
    fsinfo[492..496].copy_from_slice(&3u32.to_le_bytes()); // Next free
    fsinfo[508..512].copy_from_slice(&0xAA550000u32.to_le_bytes()); // Trail sig

    // Write Boot Area (Sectors 0-8)
    drive.seek(SeekFrom::Start(0)).unwrap();
    drive.write_all(&boot).unwrap(); // Sector 0
    drive.write_all(&fsinfo).unwrap(); // Sector 1
    
    let zeros = vec![0u8; 512];
    for _ in 2..6 { drive.write_all(&zeros).unwrap(); } // Sectors 2-5
    
    drive.write_all(&boot).unwrap(); // Sector 6 (Backup)
    drive.write_all(&fsinfo).unwrap(); // Sector 7 (Backup)
    
    // Zero rest of reserved sectors
    for _ in 8..reserved_sectors { drive.write_all(&zeros).unwrap(); }

    // Write FAT Tables
    let fat_byte_offset = reserved_sectors as u64 * bytes_per_sector;
    drive.seek(SeekFrom::Start(fat_byte_offset)).unwrap();
    
    // Create initial FAT cluster entries (Media type + End of Chain + Root Dir)
    let mut fat_init = vec![0u8; 12];
    fat_init[0..4].copy_from_slice(&0x0FFFFFF8u32.to_le_bytes()); // Cluster 0: Media
    fat_init[4..8].copy_from_slice(&0x0FFFFFFFu32.to_le_bytes()); // Cluster 1: EOC
    fat_init[8..12].copy_from_slice(&0x0FFFFFF8u32.to_le_bytes()); // Cluster 2: Root EOC

    let fat_size_bytes = sectors_per_fat as u64 * bytes_per_sector;
    let fat_zeros = vec![0u8; (fat_size_bytes - 12) as usize];

    for _ in 0..num_fats {
        drive.write_all(&fat_init).unwrap();
        drive.write_all(&fat_zeros).unwrap();
    }

    // Write Root Directory (Cluster 2)
    let root_dir_offset = fat_byte_offset + (num_fats as u64 * fat_size_bytes);
    drive.seek(SeekFrom::Start(root_dir_offset)).unwrap();
    
    // Write Volume Label Entry as the first file in the root directory
    let mut root_entry = vec![0u8; (sectors_per_cluster * bytes_per_sector as u32) as usize];
    root_entry[0..11].copy_from_slice(&label_bytes);
    root_entry[11] = 0x08; // Volume ID attribute
    drive.write_all(&root_entry).unwrap();

    drive.flush().unwrap();
    Ok(())
}





// ──  FAT32 Engine 

pub struct BareFat32<T: Read + Write + Seek> {
    pub inner: Mutex<T>,
    pub bytes_per_cluster: u64,
    pub fat_offset: u64,
    pub heap_offset: u64,
    pub root_cluster: u32,
    pub cluster_count: u32,
    pub dir_map: Mutex<HashMap<String, u32>>,
    pub file_map: Mutex<HashMap<String, (u32, u64)>>,
    pub next_free: Mutex<u32>,
}


fn split_path(path: &str) -> (&str, &str) {
    let path = path.trim_matches('/');
    match path.rfind('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => ("", path),
    }
}

impl<T: Read + Write + Seek> BareFat32<T> {

    pub fn mount(mut inner: T) -> Result<Self, String> {
        let mut boot = [0u8; 512];
        inner.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
        inner.read_exact(&mut boot).map_err(|e| e.to_string())?;

        let bytes_per_sector = u16::from_le_bytes(boot[11..13].try_into().unwrap()) as u64;
        let sectors_per_cluster = boot[13] as u64;
        let reserved_sectors = u16::from_le_bytes(boot[14..16].try_into().unwrap()) as u64;
        let num_fats = boot[16] as u64;
        let total_sectors_32 = u32::from_le_bytes(boot[32..36].try_into().unwrap()) as u64;
        let sectors_per_fat = u32::from_le_bytes(boot[36..40].try_into().unwrap()) as u64;
        let root_cluster = u32::from_le_bytes(boot[44..48].try_into().unwrap());

        let bytes_per_cluster = bytes_per_sector * sectors_per_cluster;
        let fat_offset = reserved_sectors * bytes_per_sector;
        let heap_offset = fat_offset + (num_fats * sectors_per_fat * bytes_per_sector);
        let cluster_count = (total_sectors_32 * bytes_per_sector - heap_offset) / bytes_per_cluster;

        Ok(Self {
            inner: Mutex::new(inner),
            bytes_per_cluster,
            fat_offset,
            heap_offset,
            root_cluster,
            cluster_count: cluster_count as u32,
            dir_map: Mutex::new(HashMap::new()),
            file_map: Mutex::new(HashMap::new()),
            next_free: Mutex::new(3), // Cluster 2 is the root dir, so 3 is the next free
        })
    }


    fn alloc_clusters(&self, bytes: u64) -> Result<u32, String> {

        if bytes == 0 { return Ok(0); }
        let clusters_needed = (bytes + self.bytes_per_cluster - 1) / self.bytes_per_cluster;
        
        let mut inner = self.inner.lock().unwrap();
        let mut next_free = self.next_free.lock().unwrap();
        
        let mut allocated = Vec::new();
        let mut current = *next_free;
        
        while allocated.len() < clusters_needed as usize {
            if current > self.cluster_count + 2 {
                return Err("FAT32 Drive Full!".to_string());
            }
            inner.seek(SeekFrom::Start(self.fat_offset + (current as u64 * 4))).unwrap();
            let mut entry = [0u8; 4];
            inner.read_exact(&mut entry).unwrap();
            
            if (u32::from_le_bytes(entry) & 0x0FFFFFFF) == 0 {
                allocated.push(current);
            }
            current += 1;
        }

        // Link the clusters in the FAT table
        for i in 0..allocated.len() {
            let c = allocated[i];
            let next = if i == allocated.len() - 1 { 0x0FFFFFFF } else { allocated[i+1] };
            inner.seek(SeekFrom::Start(self.fat_offset + (c as u64 * 4))).unwrap();
            inner.write_all(&next.to_le_bytes()).unwrap();
        }

        *next_free = current;
        Ok(allocated[0])
    }


    fn append_entries(&self, dir_cluster: u32, entries: &[[u8; 32]]) -> Result<(), String> {
        
        let mut inner = self.inner.lock().unwrap();
        let mut current_cluster = dir_cluster;
        
        loop {
            let offset = self.heap_offset + (current_cluster as u64 - 2) * self.bytes_per_cluster;
            inner.seek(SeekFrom::Start(offset)).unwrap();
            let mut cluster_data = vec![0u8; self.bytes_per_cluster as usize];
            inner.read_exact(&mut cluster_data).unwrap();

            let mut consecutive_free = 0;
            let mut start_idx = 0;

            for i in 0..(self.bytes_per_cluster as usize / 32) {
                let b = cluster_data[i * 32];
                if b == 0x00 || b == 0xE5 {
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

            // Directory cluster is full. Read the FAT to find the next cluster in the chain.
            inner.seek(SeekFrom::Start(self.fat_offset + (current_cluster as u64 * 4))).unwrap();
            let mut fat_entry = [0u8; 4];
            inner.read_exact(&mut fat_entry).unwrap();
            let next = u32::from_le_bytes(fat_entry) & 0x0FFFFFFF;
            
            if next >= 0x0FFFFFF8 {
                // Reached end of directory chain, allocate a new cluster to expand the directory!
                drop(inner); 
                let new_cluster = self.alloc_clusters(self.bytes_per_cluster)?;
                inner = self.inner.lock().unwrap(); 
                
                inner.seek(SeekFrom::Start(self.fat_offset + (current_cluster as u64 * 4))).unwrap();
                inner.write_all(&new_cluster.to_le_bytes()).unwrap();
                
                let new_offset = self.heap_offset + (new_cluster as u64 - 2) * self.bytes_per_cluster;
                inner.seek(SeekFrom::Start(new_offset)).unwrap();
                inner.write_all(&vec![0u8; self.bytes_per_cluster as usize]).unwrap();
                
                current_cluster = new_cluster;
            } else {
                current_cluster = next;
            }
        }
    }

    
    // Walks the entire directory tree and returns a formatted list of all files and folders
    pub fn inspect_all(&self) -> Result<Vec<String>, String> {

        let mut found_files = Vec::new();
        self.walk_dir(self.root_cluster, "", &mut found_files)?;
        Ok(found_files)
    }


    // Recursive helper to walk directory clusters and reconstruct Long File Names (LFN)
    fn walk_dir(&self, start_cluster: u32, current_path: &str, found_files: &mut Vec<String>) -> Result<(), String> {
        let mut current_cluster = start_cluster;
        let mut lfn_buffer = HashMap::new(); 

        loop {
            let offset = if current_cluster >= 2 {
                self.heap_offset + (current_cluster as u64 - 2) * self.bytes_per_cluster
            } else {
                return Err(format!("Invalid cluster jump: {}", current_cluster));
            };

            let mut dir_data = vec![0u8; self.bytes_per_cluster as usize];
            {
                let mut inner = self.inner.lock().unwrap();
                inner.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
                inner.read_exact(&mut dir_data).map_err(|e| e.to_string())?;
            }

            let mut i = 0;
            while i < dir_data.len() / 32 {
                let entry = &dir_data[i * 32 .. (i + 1) * 32];
                let first_byte = entry[0];

                if first_byte == 0x00 {
                    return Ok(()); // End of directory block reached
                }
                if first_byte == 0xE5 || first_byte == 0x05 {
                    i += 1;
                    continue; // Deleted file entry, skip
                }

                let attr = entry[11];

                if attr == 0x0F {
                    // It's a Long File Name (LFN) Entry
                    let order = first_byte & 0x3F; // Strip the 0x40 'last entry' flag
                    let mut chars = Vec::new();
                    
                    // LFN characters are stored in 3 separate fragmented blocks per entry
                    for j in (1..11).step_by(2) { chars.push(u16::from_le_bytes(entry[j..j+2].try_into().unwrap())); }
                    for j in (14..26).step_by(2) { chars.push(u16::from_le_bytes(entry[j..j+2].try_into().unwrap())); }
                    for j in (28..32).step_by(2) { chars.push(u16::from_le_bytes(entry[j..j+2].try_into().unwrap())); }
                    
                    lfn_buffer.insert(order, chars);
                } else if attr & 0x08 == 0 {
                    // It's a standard 8.3 Entry (Not a Volume ID)
                    let is_dir = (attr & 0x10) != 0;
                    let cluster_hi = u16::from_le_bytes(entry[20..22].try_into().unwrap()) as u32;
                    let cluster_lo = u16::from_le_bytes(entry[26..28].try_into().unwrap()) as u32;
                    let target_cluster = (cluster_hi << 16) | cluster_lo;
                    let file_size = u32::from_le_bytes(entry[28..32].try_into().unwrap());

                    // Reconstruct the real file name
                    let name = if !lfn_buffer.is_empty() {
                        let mut orders: Vec<u8> = lfn_buffer.keys().cloned().collect();
                        orders.sort();
                        let mut utf16_chars = Vec::new();
                        for o in orders { utf16_chars.extend_from_slice(&lfn_buffer[&o]); }
                        
                        // Strip unused 0x0000 and 0xFFFF padding chars
                        utf16_chars.retain(|&c| c != 0x0000 && c != 0xFFFF);
                        let lfn_name = String::from_utf16_lossy(&utf16_chars);
                        lfn_buffer.clear();
                        
                        lfn_name // Returns the LFN string out of this block
                    } else {
                        // 8.3 Name fallback
                        let stem = String::from_utf8_lossy(&entry[0..8]).trim().to_string();
                        let ext = String::from_utf8_lossy(&entry[8..11]).trim().to_string();
                        if ext.is_empty() { stem } else { format!("{}.{}", stem, ext) } // Returns the SFN string out of this block
                    };

                    if name != "." && name != ".." && !name.is_empty() {
                        let full_path = if current_path.is_empty() { format!("/{}", name) } else { format!("{}/{}", current_path, name) };
                        if is_dir {
                            found_files.push(format!("[DIR ] {}", full_path));
                            if target_cluster >= 2 {
                                self.walk_dir(target_cluster, &full_path, found_files)?;
                            }
                        } else {
                            found_files.push(format!("[FILE] {} ({} bytes)", full_path, file_size));
                        }
                    }
                }
                i += 1;
            }

            // Move to the next cluster in the directory chain
            let mut inner = self.inner.lock().unwrap();
            inner.seek(SeekFrom::Start(self.fat_offset + (current_cluster as u64 * 4))).unwrap();
            let mut fat_entry = [0u8; 4];
            inner.read_exact(&mut fat_entry).unwrap();
            
            let next = u32::from_le_bytes(fat_entry) & 0x0FFFFFFF;
            if next >= 0x0FFFFFF8 { break; } // End of chain
            current_cluster = next;
        }
        Ok(())
    }
}


fn build_fat32_entry_set(name: &str, cluster: u32, size: u64, is_dir: bool) -> Vec<[u8; 32]> {
    
    let mut entries = Vec::new();
    
    // Generate 8.3 Short File Name (SFN)
    let mut sfn = [b' '; 11];
    let name_upper = name.to_ascii_uppercase();
    let (stem, ext) = if is_dir { (name_upper.as_str(), "") } else {
        let parts: Vec<&str> = name_upper.rsplitn(2, '.').collect();
        if parts.len() == 2 { (parts[1], parts[0]) } else { (name_upper.as_str(), "") }
    };

    let clean_stem: String = stem.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-').collect();
    let clean_ext: String = ext.chars().filter(|c| c.is_ascii_alphanumeric()).collect();

    let stem_bytes = clean_stem.as_bytes();
    let copy_len = std::cmp::min(6, stem_bytes.len());
    sfn[..copy_len].copy_from_slice(&stem_bytes[..copy_len]);
    sfn[6] = b'~';
    sfn[7] = b'1'; 

    let ext_bytes = clean_ext.as_bytes();
    let ext_len = std::cmp::min(3, ext_bytes.len());
    sfn[8..8+ext_len].copy_from_slice(&ext_bytes[..ext_len]);

    // Compute SFN Checksum for LFN Binding
    let mut checksum: u8 = 0;
    for &b in &sfn { checksum = (checksum.rotate_right(1)).wrapping_add(b); }

    // Generate Long File Name (LFN) Entries
    let utf16: Vec<u16> = name.encode_utf16().collect();
    let num_lfn = (utf16.len() + 12) / 13;

    for i in (0..num_lfn).rev() {
        let mut lfn = [0u8; 32];
        lfn[0] = (i + 1) as u8 | if i == num_lfn - 1 { 0x40 } else { 0x00 };
        lfn[11] = 0x0F; // LFN Attribute Flag
        lfn[13] = checksum;

        let start = i * 13;
        let mut chars = [0xFFFFu16; 13];
        for j in 0..13 {
            if start + j < utf16.len() {
                chars[j] = utf16[start + j];
            } else if start + j == utf16.len() {
                chars[j] = 0x0000;
            }
        }

        for j in 0..5 { lfn[1 + j*2 .. 3 + j*2].copy_from_slice(&chars[j].to_le_bytes()); }
        for j in 0..6 { lfn[14 + j*2 .. 16 + j*2].copy_from_slice(&chars[5 + j].to_le_bytes()); }
        for j in 0..2 { lfn[28 + j*2 .. 30 + j*2].copy_from_slice(&chars[11 + j].to_le_bytes()); }

        entries.push(lfn);
    }

    // Generate standard 8.3 entry
    let mut sfn_entry = [0u8; 32];
    sfn_entry[0..11].copy_from_slice(&sfn);
    sfn_entry[11] = if is_dir { 0x10 } else { 0x20 };
    
    let cluster_hi = (cluster >> 16) as u16;
    let cluster_lo = (cluster & 0xFFFF) as u16;
    sfn_entry[20..22].copy_from_slice(&cluster_hi.to_le_bytes());
    sfn_entry[26..28].copy_from_slice(&cluster_lo.to_le_bytes());
    sfn_entry[28..32].copy_from_slice(&(size as u32).to_le_bytes());

    entries.push(sfn_entry);
    entries
}




// -- Aux structs and trait implelentations for writer / verifier

use crate::writer::{UsbWriter};
use crate::verify::{UsbReader};

pub struct BareFatFileWriter<'w, T: Read + Write + Seek> {
    fs: &'w BareFat32<T>,
    start_offset: u64,
    bytes_written: u64,
    max_size: u64,
}

impl<'w, T: Read + Write + Seek> Write for BareFatFileWriter<'w, T> {
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

impl<T: Read + Write + Seek> UsbWriter for BareFat32<T> {
    type FileWriter<'w> = BareFatFileWriter<'w, T> where Self: 'w;

    fn create_dir(&self, path: &str) -> Result<(), String> {
        let (parent_path, name) = crate::fat32::split_path(path);
        let mut dir_map = self.dir_map.lock().unwrap();
        let parent_cluster = *dir_map.get(parent_path).unwrap_or(&self.root_cluster);
        
        let new_cluster = self.alloc_clusters(self.bytes_per_cluster)?;
        
        // Zero out the new directory
        let offset = self.heap_offset + (new_cluster as u64 - 2) * self.bytes_per_cluster;
        {
            let mut inner = self.inner.lock().unwrap();
            inner.seek(SeekFrom::Start(offset)).unwrap();
            inner.write_all(&vec![0u8; self.bytes_per_cluster as usize]).unwrap();
        }
        
        let entries = crate::fat32::build_fat32_entry_set(name, new_cluster, 0, true);
        self.append_entries(parent_cluster, &entries)?;
        
        dir_map.insert(path.trim_matches('/').to_string(), new_cluster);
        Ok(())
    }

    fn open_file_writer<'w>(&'w self, path: &str, size: u64) -> Result<Self::FileWriter<'w>, String> {
        let (parent_path, name) = crate::fat32::split_path(path);
        let dir_map = self.dir_map.lock().unwrap();
        let parent_cluster = *dir_map.get(parent_path).unwrap_or(&self.root_cluster);
        
        let file_cluster = self.alloc_clusters(size)?;
        let entries = crate::fat32::build_fat32_entry_set(name, file_cluster, size, false);
        self.append_entries(parent_cluster, &entries)?;
        
        // Cache the file location in memory so we NEVER have to parse the FAT table during verification!
        self.file_map.lock().unwrap().insert(path.trim_matches('/').to_string(), (file_cluster, size));

        let start_offset = if file_cluster >= 2 {
            self.heap_offset + (file_cluster as u64 - 2) * self.bytes_per_cluster
        } else { 0 };
        
        Ok(BareFatFileWriter { fs: self, start_offset, bytes_written: 0, max_size: size })
    }
}

pub struct BareFatFileReader<'r, T: Read + Write + Seek> {
    fs: &'r BareFat32<T>,
    start_offset: u64,
    size: u64,
    position: u64,
}

impl<'r, T: Read + Write + Seek> Read for BareFatFileReader<'r, T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.size { return Ok(0); }
        let mut inner = self.fs.inner.lock().unwrap();
        inner.seek(SeekFrom::Start(self.start_offset + self.position))?;
        
        let to_read = std::cmp::min(buf.len() as u64, self.size - self.position) as usize;
        inner.read_exact(&mut buf[..to_read])?;
        self.position += to_read as u64;
        Ok(to_read)
    }
}

impl<T: Read + Write + Seek> UsbReader for BareFat32<T> {
    type FileReader<'r> = BareFatFileReader<'r, T> where Self: 'r;

    fn get_file_size(&self, path: &str) -> Result<u64, String> {
        let clean_path = path.trim_matches('/');
        let file_map = self.file_map.lock().unwrap();
        if let Some(&(_, size)) = file_map.get(clean_path) { Ok(size) } else { Err(format!("Not found: {}", path)) }
    }

    fn open_file_reader<'r>(&'r self, path: &str) -> Result<Self::FileReader<'r>, String> {
        let clean_path = path.trim_matches('/');
        let file_map = self.file_map.lock().unwrap();
        if let Some(&(cluster, size)) = file_map.get(clean_path) {
            let start_offset = if cluster >= 2 { self.heap_offset + (cluster as u64 - 2) * self.bytes_per_cluster } else { 0 };
            Ok(BareFatFileReader { fs: self, start_offset, size, position: 0 })
        } else {
            Err(format!("Not found: {}", path))
        }
    }
}

