use std::{
    io::{Read, Seek, SeekFrom, Result, Error, ErrorKind}, 
    collections::HashSet
};

/*
# Etract files from SquashFS flow - 
- Read the Superblock to map the disk
- Follow the 64-bit pointer into the Inode Table to find the Root Directory
- Sweep through the compressed Directory Table 
- Grab file (eg vfat.ko.zst) cooridnates
- Jump back to the Inode Table to read the file's Blueprint
- Jump to the raw Data Blocks, unpack, and reassembled the driver in RAM
*/


// SquashFS 4.0 Superblock is 96 bytes long
pub const SUPERBLOCK_SIZE: usize = 96;
pub const SQUASHFS_MAGIC: u32 = 0x73717368; // "hsqs" in Little-Endian

const METADATA_UNCOMPRESSED_FLAG: u16 = 0x8000; // The 16th bit (1000 0000 0000 0000)
const METADATA_SIZE_MASK: u16 = 0x7FFF;         // The lower 15 bits (0111 1111 1111 1111)

const DATA_UNCOMPRESSED_FLAG: u32 = 0x01000000; // The 25th bit
const DATA_SIZE_MASK: u32 = 0x00FFFFFF;         // The lower 24 bits


#[derive(Debug)]
pub struct Superblock {
    pub magic: u32,
    pub inode_count: u32,
    pub mod_time: u32,
    pub block_size: u32,
    pub frag_count: u32,
    pub compressor: u16,
    pub block_log: u16,
    pub flags: u16,
    pub id_count: u16,
    pub version_major: u16,
    pub version_minor: u16,
    pub root_inode: u64,
    pub bytes_used: u64,
    pub id_table_start: u64,
    pub xattr_table_start: u64,
    pub inode_table_start: u64,
    pub directory_table_start: u64,
    pub fragment_table_start: u64,
    pub export_table_start: u64,
}


// Standard 16-byte header attached to every single file and folder in SquashFS
#[derive(Debug)]
pub struct InodeHeader {
    pub inode_type: u16,
    pub permissions: u16,
    pub uid: u16,
    pub gid: u16,
    pub mtime: u32,
    pub inode_number: u32,
}

// A unified struct for Basic and Extended directories
#[derive(Debug)]
pub struct DirectoryLocation {
    pub block_index: u32,  // Offset into the Directory Table
    pub block_offset: u16, // Decompressed byte offset inside the Directory Table block
    pub file_size: u32,    // Total size of the directory entries we need to read
}

#[derive(Debug)]
pub struct FileLocation {
    pub name: String,
    pub block_index: u32,
    pub block_offset: u16,
}

#[derive(Debug)]
pub struct FileBlueprint {
    pub blocks_start: u64, // Physical disk offset where data begins
    pub file_size: u32,    // Total uncompressed size of the file
    pub block_sizes: Vec<u32>, // Array of block sizes (with the 24-bit flag)
    pub frag_index: u32,   // 0xFFFFFFFF if no fragment is used
    pub block_offset: u32, // Offset inside the fragment
}


#[derive(Debug)]
pub struct FragmentEntry {
    pub start: u64,       // Physical offset where the massive fragment block lives
    pub size: u32,        // Block size + the 25th-bit Compression Flag
    pub unused: u32,      // Padding
}


// -- Endianness Helpers 

fn read_u16(bytes: &[u8], is_le: bool) -> u16 {
    let b = bytes.try_into().unwrap();
    if is_le { u16::from_le_bytes(b) } else { u16::from_be_bytes(b) }
}

fn read_u32(bytes: &[u8], is_le: bool) -> u32 {
    let b = bytes.try_into().unwrap();
    if is_le { u32::from_le_bytes(b) } else { u32::from_be_bytes(b) }
}

fn read_u64(bytes: &[u8], is_le: bool) -> u64 {
    let b = bytes.try_into().unwrap();
    if is_le { u64::from_le_bytes(b) } else { u64::from_be_bytes(b) }
}

// -- 

pub fn read_superblock<R: Read + Seek>(mut reader: R) -> Result<Superblock> {

    let mut buf = [0u8; SUPERBLOCK_SIZE];
    
    // Seek to the beginning of the file/partition
    reader.seek(SeekFrom::Start(0))?;
    reader.read_exact(&mut buf)?;

    // Read the magic bytes first to fail fast if it's not a SquashFS image
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != SQUASHFS_MAGIC {
        return Err(Error::new(
            ErrorKind::InvalidData, 
            format!("Invalid magic bytes. Expected {:#0x}, got {:#0x}", SQUASHFS_MAGIC, magic)
        ));
    }

    // Unpack the remaining 92 bytes using standard Little-Endian math
    Ok(Superblock {
        magic,
        inode_count: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        mod_time: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        block_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        frag_count: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        compressor: u16::from_le_bytes(buf[20..22].try_into().unwrap()),
        block_log: u16::from_le_bytes(buf[22..24].try_into().unwrap()),
        flags: u16::from_le_bytes(buf[24..26].try_into().unwrap()),
        id_count: u16::from_le_bytes(buf[26..28].try_into().unwrap()),
        version_major: u16::from_le_bytes(buf[28..30].try_into().unwrap()),
        version_minor: u16::from_le_bytes(buf[30..32].try_into().unwrap()),
        root_inode: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
        bytes_used: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
        id_table_start: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
        xattr_table_start: u64::from_le_bytes(buf[56..64].try_into().unwrap()),
        inode_table_start: u64::from_le_bytes(buf[64..72].try_into().unwrap()),
        directory_table_start: u64::from_le_bytes(buf[72..80].try_into().unwrap()),
        fragment_table_start: u64::from_le_bytes(buf[80..88].try_into().unwrap()),
        export_table_start: u64::from_le_bytes(buf[88..96].try_into().unwrap()),
    })
}



pub fn read_metadata_block<R: Read + Seek>(reader: &mut R, is_little_endian: bool,
) -> Result<Vec<u8>> {
    
    // Read the 2-byte header
    let mut header_bytes = [0u8; 2];
    reader.read_exact(&mut header_bytes)?;

    let raw_header = if is_little_endian {
        u16::from_le_bytes(header_bytes)
    } else {
        u16::from_be_bytes(header_bytes)
    };

    // Unpack the bits
    let is_uncompressed = (raw_header & METADATA_UNCOMPRESSED_FLAG) != 0;
    let compressed_size = (raw_header & METADATA_SIZE_MASK) as usize;

    // Sanity check: SquashFS metadata blocks cannot exceed 8KB (8192 bytes)
    if compressed_size == 0 || compressed_size > 8192 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("Corrupt metadata block size: {}", compressed_size),
        ));
    }

    // Read the payload from the disk
    let mut payload = vec![0u8; compressed_size];
    reader.read_exact(&mut payload)?;

    // Return or Decompress
    if is_uncompressed {
        Ok(payload)
    } else {
        // assume ZSTD 
        zstd::stream::decode_all(std::io::Cursor::new(payload))
    }
}



pub fn read_root_inode<R: Read + Seek>(reader: &mut R, superblock: &Superblock, is_le: bool, 
) -> Result<(InodeHeader, DirectoryLocation)> {
    
    // Upper 48 bits - offset from the Inode table. Lower 16 - byte offset within the block
    let block_index = (superblock.root_inode >> 16) as u64;
    let byte_offset = (superblock.root_inode & 0xFFFF) as usize;

    // Seek to the exact metadata block in the Inode Table
    reader.seek(SeekFrom::Start(superblock.inode_table_start + block_index))?;

    // Read and decompress the block
    let inode_data = read_metadata_block(reader, is_le)?;

    // Slice into the uncompressed data at the byte offset
    if byte_offset + 16 > inode_data.len() {
        return Err(Error::new(ErrorKind::InvalidData, "Inode crosses metadata boundary"));
    }
    let data = &inode_data[byte_offset..];

    // Parse the 16-byte Inode Header
    let header = InodeHeader {
        inode_type: read_u16(&data[0..2], is_le),
        permissions: read_u16(&data[2..4], is_le),
        uid: read_u16(&data[4..6], is_le),
        gid: read_u16(&data[6..8], is_le),
        mtime: read_u32(&data[8..12], is_le),
        inode_number: read_u32(&data[12..16], is_le),
    };

    // Parse the specific Directory Payload (Basic = 1, Extended = 8)
    let payload = &data[16..];
    
    let location = match header.inode_type {
        1 => {
            // Basic Directory (16 byte payload)
            DirectoryLocation {
                block_index: read_u32(&payload[0..4], is_le),
                file_size: read_u16(&payload[8..10], is_le) as u32,
                block_offset: read_u16(&payload[10..12], is_le),
            }
        }
        8 => {
            // Extended Directory (24 byte payload)
            DirectoryLocation {
                file_size: read_u32(&payload[4..8], is_le),
                block_index: read_u32(&payload[8..12], is_le),
                block_offset: read_u16(&payload[18..20], is_le),
            }
        }
        _ => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                format!("Expected Directory Inode, got type: {}", header.inode_type)
            ));
        }
    };

    Ok((header, location))
}


pub fn traverse_directory<R: Read + Seek>(
    reader: &mut R,
    superblock: &Superblock,
    dir_loc: &DirectoryLocation,
    hit_list: &mut HashSet<String>,
    is_le: bool,
) -> Result<Vec<FileLocation>> {

    let mut found_files = Vec::new();

    // Seek to the start of the Directory Table + our specific block offset
    let physical_offset = superblock.directory_table_start + (dir_loc.block_index as u64);
    reader.seek(SeekFrom::Start(physical_offset))?;

    /*
    Decompression - Directories can be larger than a single 8KB block. `reader.read()` 
    advances the file cursor automatically, so we keep reading metadata until enough 
    uncompressed bytes have been accumulated
    */

    let mut dir_data = Vec::new();
    let target_size = dir_loc.block_offset as usize + dir_loc.file_size as usize - 3; 
    
    while dir_data.len() < target_size {
        let mut block = read_metadata_block(reader, is_le)?;
        dir_data.append(&mut block);
    }

    // Slice into the uncompressed data at our exact starting offset
    let payload = &dir_data[dir_loc.block_offset as usize .. target_size];
    let mut cursor = 0;

    while cursor + 12 <= payload.len() && !hit_list.is_empty() {
        
        // DIR header (12 Bytes) 
        // The spec stores (count - 1), so we add 1 to get the true count
        let count = read_u32(&payload[cursor..cursor+4], is_le) + 1; 
        let start = read_u32(&payload[cursor+4..cursor+8], is_le);
        // We skip inode_num (bytes 8..12) as we don't strictly need it for this
        cursor += 12;

        // DIR entries
        for _ in 0..count {
            if cursor + 8 > payload.len() { break; } // Bounds safety
            
            let offset = read_u16(&payload[cursor..cursor+2], is_le);
            let _file_type = read_u16(&payload[cursor+4..cursor+6], is_le); // 1 = Dir, 2 = File
            
            // The spec stores (name_size - 1), so we add 1
            let name_size = read_u16(&payload[cursor+6..cursor+8], is_le) as usize + 1;
            cursor += 8;

            if cursor + name_size > payload.len() { break; } // Bounds safety
            
            let name_bytes = &payload[cursor..cursor+name_size];
            let name = String::from_utf8_lossy(name_bytes).to_string();
            cursor += name_size;

            // Pop the file if found
            if hit_list.contains(&name) {
                found_files.push(FileLocation {
                    name: name.clone(),
                    block_index: start,
                    block_offset: offset,
                });
                
                hit_list.remove(&name);
                
                if hit_list.is_empty() {
                    break; 
                }
            }
        }
    }

    Ok(found_files)
}



pub fn read_file_inode<R: Read + Seek>(
    reader: &mut R, superblock: &Superblock, block_index: u32, block_offset: u16, is_le: bool,
) -> Result<FileBlueprint> {
    
    // Seek and decompress the Inode block
    let physical_offset = superblock.inode_table_start + (block_index as u64);
    reader.seek(SeekFrom::Start(physical_offset))?;
    
    let inode_data = read_metadata_block(reader, is_le)?;
    let data = &inode_data[block_offset as usize ..];

    // Skip the 16-byte generic InodeHeader (we don't need permissions/UIDs here)
    let payload = &data[16..];

    // Parse the BasicFile payload
    let blocks_start = read_u32(&payload[0..4], is_le) as u64;
    let frag_index = read_u32(&payload[4..8], is_le);
    let block_offset_frag = read_u32(&payload[8..12], is_le);
    let file_size = read_u32(&payload[12..16], is_le);

    // Calculate how many data blocks we have
    // If there is no fragment (0xFFFFFFFF), the tail of the file is in a normal block
    // If there IS a fragment, the tail is missing from this array
    let block_count = if frag_index == 0xFFFFFFFF {
        (file_size + superblock.block_size - 1) / superblock.block_size
    } else {
        file_size / superblock.block_size
    };

    // Read the array of block sizes
    let mut block_sizes = Vec::new();
    let mut cursor = 16;
    for _ in 0..block_count {
        block_sizes.push(read_u32(&payload[cursor..cursor+4], is_le));
        cursor += 4;
    }

    Ok(FileBlueprint {
        blocks_start,
        file_size,
        block_sizes,
        frag_index,
        block_offset: block_offset_frag,
    })
}


pub fn read_fragment_entry<R: Read + Seek>(
    reader: &mut R, superblock: &Superblock, frag_index: u32, is_le: bool,
) -> Result<FragmentEntry> {
    
    // A fragment metadata block holds 512 entries (8192 / 16)
    let lookup_index = frag_index / 512;
    let entry_offset = (frag_index % 512) * 16;

    // Seek to the u64 pointer in the Lookup Table
    let lookup_table_offset = superblock.fragment_table_start + (lookup_index as u64 * 8);
    reader.seek(SeekFrom::Start(lookup_table_offset))?;

    let mut ptr_buf = [0u8; 8];
    reader.read_exact(&mut ptr_buf)?;
    let metadata_block_ptr = read_u64(&ptr_buf, is_le);

    // Jump to the Metadata block and decompress it using our engine
    reader.seek(SeekFrom::Start(metadata_block_ptr))?;
    let metadata_block = read_metadata_block(reader, is_le)?;

    // Slice out a 16-byte FragmentEntry
    let data = &metadata_block[entry_offset as usize .. (entry_offset + 16) as usize];
    
    Ok(FragmentEntry {
        start: read_u64(&data[0..8], is_le),
        size: read_u32(&data[8..12], is_le),
        unused: read_u32(&data[12..16], is_le),
    })
}


pub fn extract_file_data<R: Read + Seek>(
    reader: &mut R, superblock: &Superblock, blueprint: &FileBlueprint, is_le: bool,                  
) -> Result<Vec<u8>> {
    
    let mut file_contents = Vec::with_capacity(blueprint.file_size as usize);

    reader.seek(SeekFrom::Start(blueprint.blocks_start))?;

    for &raw_size in &blueprint.block_sizes {
        
        let is_uncompressed = (raw_size & DATA_UNCOMPRESSED_FLAG) != 0;
        let read_size = (raw_size & DATA_SIZE_MASK) as usize;

        let mut block = vec![0u8; read_size];
        reader.read_exact(&mut block)?;

        // Decompress or Append
        if is_uncompressed {
            file_contents.extend_from_slice(&block);
        } else {
            // assuming ZSTD 
            let uncompressed_block = zstd::stream::decode_all(std::io::Cursor::new(block))?;
            file_contents.extend_from_slice(&uncompressed_block);
        }
    }

    // The fragment tail
    // 0xFFFFFFFF means the file fit perfectly into normal blocks
    if blueprint.frag_index != 0xFFFFFFFF {
        
        // Get the map to the Fragment Block
        let frag_entry = read_fragment_entry(reader, superblock, blueprint.frag_index, is_le)?;
        
        let is_uncompressed = (frag_entry.size & DATA_UNCOMPRESSED_FLAG) != 0;
        let read_size = (frag_entry.size & DATA_SIZE_MASK) as usize;

        // Rip the shared Fragment Block and decompress into RAM
        reader.seek(SeekFrom::Start(frag_entry.start))?;
        let mut frag_block = vec![0u8; read_size];
        reader.read_exact(&mut frag_block)?;

        let decompressed_frag = if is_uncompressed {
            frag_block
        } else {
            // assuming ZSTD 
            zstd::stream::decode_all(std::io::Cursor::new(frag_block))?
        };

        // This decompressed block contains the tails of DOZENS of different files
        // We use the `block_offset` to find where it starts
        let bytes_remaining = blueprint.file_size as usize - file_contents.len();
        
        let start = blueprint.block_offset as usize;
        let end = start + bytes_remaining;
        
        // Grab our bytes and append to our file
        file_contents.extend_from_slice(&decompressed_frag[start..end]);
    }

    Ok(file_contents)
}


