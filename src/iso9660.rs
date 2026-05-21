use std::io::{Read, Seek, SeekFrom, Result, Error, ErrorKind};


pub const SECTOR_SIZE: u64 = 2048;


#[derive(Debug, Clone)]
pub struct DirectoryRef {
    pub lba: u32,
    pub size: u32,
    pub is_joliet: bool,
}

#[derive(Debug, Clone)]
pub struct IsoNode {
    pub name: String,
    pub is_dir: bool,
    pub extents: Vec<DirectoryRef>,
}



/// Parses the SUSP / Rock Ridge metadata to find the real Linux filename.
/// It seamlessly jumps across the disk if it hits a Continuation Entry (CE).
fn parse_rock_ridge_name<R: Read + Seek>(
    reader: &mut R, su_data: Vec<u8>, mut name_parts: Vec<u8>
) -> Result<String> {
    
    let mut cursor = 0;
    
    while cursor + 4 <= su_data.len() {
        let tag = &su_data[cursor .. cursor + 2];
        let len = su_data[cursor + 2] as usize;
        
        if len == 0 || cursor + len > su_data.len() {
            break; 
        }
        
        let entry = &su_data[cursor .. cursor + len];
        cursor += len;
        
        match tag {
            b"NM" => {
                // Rock Ridge Alternate Name (NM)
                // Byte 4 is flags. 0x01 means "CONTINUE" (name is split across multiple NM tags)
                let _flags = entry[4];
                name_parts.extend_from_slice(&entry[5..]);
            }
            b"CE" => {
                // Continuation Area (CE). The metadata overflowed this directory record!
                // ISO9660 uses Both-Endian. The Little-Endian 32-bit ints are at offsets 4, 12, 20.
                let ce_lba = u32::from_le_bytes(entry[4..8].try_into().unwrap());
                let ce_offset = u32::from_le_bytes(entry[12..16].try_into().unwrap());
                let ce_length = u32::from_le_bytes(entry[20..24].try_into().unwrap());
                
                // Jump across the disk to fetch the rest of the metadata!
                let physical_offset = (ce_lba as u64 * SECTOR_SIZE) + ce_offset as u64;
                
                // Save our place in the reader so we don't break the parent directory loop
                let original_pos = reader.stream_position()?;
                
                reader.seek(SeekFrom::Start(physical_offset))?;
                let mut continuation_buf = vec![0u8; ce_length as usize];
                reader.read_exact(&mut continuation_buf)?;
                
                reader.seek(SeekFrom::Start(original_pos))?;
                
                // Recursively parse the new block!
                return parse_rock_ridge_name(reader, continuation_buf, name_parts);
            }
            b"ST" => {
                // Terminator tag. We are done!
                break;
            }
            _ => {} // Ignore PX, TF, SL, etc.
        }
    }
    
    if name_parts.is_empty() {
        Err(Error::new(ErrorKind::NotFound, "No Rock Ridge Name Found"))
    } else {
        Ok(String::from_utf8_lossy(&name_parts).into_owned())
    }
}


pub fn get_root_directory<R: Read + Seek>(reader: &mut R) -> Result<DirectoryRef> {
    
    let mut buf = [0u8; 2048];
    let mut current_sector = 16; // PVD always starts at 16
    
    loop {
        reader.seek(SeekFrom::Start(current_sector * SECTOR_SIZE))?;
        reader.read_exact(&mut buf)?;
        
        if &buf[1..6] != b"CD001" {
            return Err(Error::new(ErrorKind::InvalidData, "Missing CD001 Signature"));
        }
        
        // 1 == Primary Volume Descriptor
        if buf[0] == 1 {
            let root_record = &buf[156..190];
            return Ok(DirectoryRef {
                lba: u32::from_le_bytes(root_record[2..6].try_into().unwrap()),
                size: u32::from_le_bytes(root_record[10..14].try_into().unwrap()),
                is_joliet: false,
            });
        }
        
        // 255 == Terminator
        if buf[0] == 255 {
            return Err(Error::new(ErrorKind::NotFound, "No Primary Volume Descriptor found"));
        }
        
        current_sector += 1;
    }
}


pub fn get_joliet_root_directory<R: Read + Seek>(reader: &mut R) -> Result<Option<DirectoryRef>> {
    
    let mut buf = [0u8; 2048];
    let mut current_sector = 16;
    
    loop {
        reader.seek(SeekFrom::Start(current_sector * SECTOR_SIZE))?;
        reader.read_exact(&mut buf)?;
        
        if &buf[1..6] != b"CD001" {
            return Err(Error::new(ErrorKind::InvalidData, "Missing CD001 Signature"));
        }
        
        // 2 == Supplementary Volume Descriptor (SVD)
        if buf[0] == 2 {
            // Bytes 88-119 contain the Escape Sequences. 
            // Joliet levels 1, 2, and 3 use %/@, %/C, and %/E respectively.
            let escape = &buf[88..91];
            if escape == b"%/@" || escape == b"%/C" || escape == b"%/E" {
                let root_record = &buf[156..190];
                return Ok(Some(DirectoryRef {
                    lba: u32::from_le_bytes(root_record[2..6].try_into().unwrap()),
                    size: u32::from_le_bytes(root_record[10..14].try_into().unwrap()),
                    is_joliet: true, // We are in the Joliet universe!
                }));
            }
        }
        
        if buf[0] == 255 {
            break; // Terminator reached
        }
        
        current_sector += 1;
    }
    
    Ok(None)
}



pub fn read_directory<R: Read + Seek>(reader: &mut R, dir: &DirectoryRef) -> Result<Vec<IsoNode>> {
    
    let mut nodes: Vec<IsoNode> = Vec::new();
    
    reader.seek(SeekFrom::Start((dir.lba as u64) * SECTOR_SIZE))?;
    let mut dir_data = vec![0u8; dir.size as usize];
    reader.read_exact(&mut dir_data)?;
    
    let mut cursor = 0;
    
    while cursor < dir_data.len() {
        let record_len = dir_data[cursor] as usize;
        
        if record_len == 0 {
            let sector_offset = cursor % 2048;
            if sector_offset > 0 {
                cursor += 2048 - sector_offset; 
                continue;
            } else {
                break;
            }
        }
        
        let record = &dir_data[cursor .. cursor + record_len];
        cursor += record_len;
        
        let lba = u32::from_le_bytes(record[2..6].try_into().unwrap());
        let size = u32::from_le_bytes(record[10..14].try_into().unwrap());

        let flags = record[25];
        let is_dir = (flags & 0x02) != 0;
        let name_len = record[32] as usize;
        let name_bytes = &record[33 .. 33 + name_len];
        
        if name_bytes == [0x00] || name_bytes == [0x01] {
            continue; // Skip '.' and '..'
        }
        
        let mut final_name = String::new();
        
        if dir.is_joliet {
            // --- JOLIET UNIVERSE ---
            // Joliet uses UTF-16 Big Endian. Every 2 bytes is a character.
            let chars: Vec<u16> = name_bytes.chunks_exact(2)
                .map(|c| u16::from_be_bytes([c[0], c[1]]))
                .collect();
                
            final_name = String::from_utf16_lossy(&chars);
            
            // Strip the ";1" version suffix if present
            if let Some(pos) = final_name.rfind(';') {
                final_name.truncate(pos); 
            }
            
        } else {
            // --- STANDARD / ROCK RIDGE UNIVERSE ---
            let padding = if name_len % 2 == 0 { 1 } else { 0 };
            let su_start = 33 + name_len + padding;
            
            if su_start < record_len {
                let su_data = record[su_start..].to_vec();
                if let Ok(rr_name) = parse_rock_ridge_name(reader, su_data, Vec::new()) {
                    final_name = rr_name;
                }
            }
            
            if final_name.is_empty() {
                final_name = String::from_utf8_lossy(name_bytes).into_owned();
                if let Some(pos) = final_name.rfind(';') {
                    final_name.truncate(pos); 
                }
            }
        }
        
        if let Some(last_node) = nodes.last_mut() {
            if last_node.name == final_name {
                // append the LBA to the existing one
                last_node.extents.push(DirectoryRef { lba, size, is_joliet: dir.is_joliet });
                continue;
            }
        }
        
        nodes.push(IsoNode { 
            name: final_name, 
            is_dir, 
            extents: vec![DirectoryRef { lba, size, is_joliet: dir.is_joliet }] 
        });
        
    }
    
    Ok(nodes)
}


/// Recursively find a file by path
pub fn find_iso_entry<R: Read + Seek>(reader: &mut R, root: &DirectoryRef, path: &str
) -> Result<IsoNode> {
    
    let mut current_dir = root.clone();
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    
    for (i, part) in parts.iter().enumerate() {
        let nodes = read_directory(reader, &current_dir)?;
        let mut found_node = None;
        
        for node in nodes {
            if node.name.eq_ignore_ascii_case(part) {
                found_node = Some(node);
                break;
            }
        }
        
        let node = found_node.ok_or_else(|| Error::new(ErrorKind::NotFound, format!("Not found: {}", part)))?;
        
        if i == parts.len() - 1 {
            return Ok(node);
        } else if node.is_dir {
            current_dir = node.extents[0].clone();
        } else {
            return Err(Error::new(ErrorKind::InvalidInput, format!("{} is a file, not a directory", part)));
        }
    }
    
    Err(Error::new(ErrorKind::NotFound, "Path was empty"))
}


pub fn stream_iso_file<R, F>(reader: &mut R, file: &IsoNode, mut on_chunk: F) -> Result<()>
where
    R: Read + Seek,
    F: FnMut(&[u8]) -> Result<()>,
{
    if file.is_dir {
        return Err(Error::new(ErrorKind::InvalidInput, "Cannot stream a directory"));
    }

    // 100KB buffer 
    let mut chunk_buf = vec![0u8; 100 * 1024]; 

    for extent in &file.extents {
        let mut extent_offset = 0u64;
        let extent_len = extent.size as u64;
        
        reader.seek(SeekFrom::Start((extent.lba as u64) * SECTOR_SIZE))?;

        while extent_offset < extent_len {
            let read_size = (extent_len - extent_offset).min(chunk_buf.len() as u64) as usize;
            reader.read_exact(&mut chunk_buf[..read_size])?;
            
            // Pass the bytes directly to your writer / progress bar
            on_chunk(&chunk_buf[..read_size])?;
            
            extent_offset += read_size as u64;
        }
    }
    
    Ok(())
}


// -- El Torito (Bootable CD)

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct BootImage {
    pub lba: u32,
    pub sector_count: u16,
    pub load_segment: u16,
}


pub fn _get_boot_image<R: Read + Seek>(reader: &mut R) -> Result<Option<BootImage>> {
    
    let mut buf = [0u8; 2048];
    let mut current_sector = 16;
    let catalog_lba;
    
    // Scan for the Boot Record Volume Descriptor (Type 0)
    loop {
        reader.seek(SeekFrom::Start(current_sector * SECTOR_SIZE))?;
        reader.read_exact(&mut buf)?;
        
        if &buf[1..6] != b"CD001" {
            return Err(Error::new(ErrorKind::InvalidData, "Missing CD001 Signature"));
        }
        
        if buf[0] == 0 { // 0 == El Torito Boot Record
            // Byte 71 contains the absolute LBA of the Boot Catalog
            catalog_lba = u32::from_le_bytes(buf[71..75].try_into().unwrap());
            break;
        }
        
        if buf[0] == 255 { // Terminator
            return Ok(None); // ISO is not bootable
        }
        current_sector += 1;
    }
    
    if catalog_lba == 0 {
        return Ok(None);
    }
    
    // Jump to the Boot Catalog
    reader.seek(SeekFrom::Start(catalog_lba as u64 * SECTOR_SIZE))?;
    reader.read_exact(&mut buf)?;
    
    // The catalog starts with a 32-byte Validation Entry. 
    // We skip it and read the next 32 bytes: The Default Boot Entry.
    let default_entry = &buf[32..64];
    
    // Byte 0 is the Boot Indicator. 0x88 means bootable.
    if default_entry[0] != 0x88 {
        return Err(Error::new(ErrorKind::InvalidData, "Default boot entry is not marked bootable (0x88)"));
    }
    
    Ok(Some(BootImage {
        load_segment: u16::from_le_bytes(default_entry[2..4].try_into().unwrap()),
        sector_count: u16::from_le_bytes(default_entry[6..8].try_into().unwrap()),
        lba: u32::from_le_bytes(default_entry[8..12].try_into().unwrap()),
    }))
}


pub fn _extract_boot_image<R: Read + Seek>(reader: &mut R, boot: &BootImage) -> Result<Vec<u8>> {
    
    // Multiply by 512 (not 2048!) because the El Torito spec counts in emulated 512-byte sectors
    let byte_size = (boot.sector_count as u64) * 512;
    
    let mut file_data = vec![0u8; byte_size as usize];
    
    // The LBA pointer, however, STILL points to a standard 2048-byte optical sector
    reader.seek(SeekFrom::Start((boot.lba as u64) * SECTOR_SIZE))?;
    reader.read_exact(&mut file_data)?;
    
    Ok(file_data)
}