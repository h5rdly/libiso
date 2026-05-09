use std::{
    io::{Read, Seek, SeekFrom, Result, Error, ErrorKind}, 
    collections::HashSet, 
    result::Result as StdResult, 
    fs::File,
};

use pyo3::{prelude::*, exceptions::PyRuntimeError};


// SquashFS 4.0 Superblock is 96 bytes long
pub const SUPERBLOCK_SIZE: usize = 96;
pub const SQUASHFS_MAGIC: u32 = 0x73717368; // "hsqs" in Little-Endian

const METADATA_UNCOMPRESSED_FLAG: u16 = 0x8000; // The 16th bit (1000 0000 0000 0000)
const METADATA_SIZE_MASK: u16 = 0x7FFF;         // The lower 15 bits (0111 1111 1111 1111)

const DATA_UNCOMPRESSED_FLAG: u32 = 0x01000000; // The 25th bit
const DATA_SIZE_MASK: u32 = 0x00FFFFFF;         // The lower 24 bits


#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[derive(Debug, Clone)]
pub struct DirectoryLocation {
    pub path: String,      // tracks the current directory string
    pub block_index: u32,  // Offset into the Directory Table
    pub block_offset: u16, // Decompressed byte offset inside the Directory Table block
    pub file_size: u32,    // Total size of the directory entries we need to read
}

#[derive(Debug)]
pub struct FileLocation {
    pub path: String,
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


#[allow(dead_code)]
#[derive(Debug)]
pub struct FragmentEntry {
    pub start: u64,       // Physical offset where the massive fragment block lives
    pub size: u32,        // Block size + the 25th-bit Compression Flag
    pub unused: u32,      // Padding
}


// -- Endianness helpers 

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


// -- Reading logic

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


pub fn read_dir_inode<R: Read + Seek>(
    reader: &mut R, 
    superblock: &Superblock, 
    block_index: u32, 
    block_offset: u16, 
    path: String,
    is_le: bool,
) -> Result<DirectoryLocation> {

    let physical_offset = superblock.inode_table_start + (block_index as u64);
    reader.seek(SeekFrom::Start(physical_offset))?;
    
    let mut inode_data = read_metadata_block(reader, is_le)?;
    
    // If the inode starts near the end of the 8KB block, stitch the next one
    // We need at most 40 bytes to safely parse a directory inode.
    while inode_data.len() < (block_offset as usize) + 40 {
        let mut next = read_metadata_block(reader, is_le)?;
        inode_data.append(&mut next);
    }
    
    let data = &inode_data[block_offset as usize ..];
    let inode_type = read_u16(&data[0..2], is_le);
    let payload = &data[16..];
    
    match inode_type {
        1 => Ok(DirectoryLocation {
            path,
            block_index: read_u32(&payload[0..4], is_le),
            file_size: read_u16(&payload[8..10], is_le) as u32,
            block_offset: read_u16(&payload[10..12], is_le),
        }),
        8 => Ok(DirectoryLocation {
            path,
            file_size: read_u32(&payload[4..8], is_le),
            block_index: read_u32(&payload[8..12], is_le),
            block_offset: read_u16(&payload[18..20], is_le),
        }),
        _ => Err(Error::new(ErrorKind::InvalidData, "Expected Directory Inode")),
    }
}


pub fn read_root_inode<R: Read + Seek>(
    reader: &mut R, superblock: &Superblock, is_le: bool, 
) -> Result<(InodeHeader, DirectoryLocation)> {
    
    let block_index = (superblock.root_inode >> 16) as u64;
    let byte_offset = (superblock.root_inode & 0xFFFF) as usize;

    reader.seek(SeekFrom::Start(superblock.inode_table_start + block_index))?;
    let mut inode_data = read_metadata_block(reader, is_le)?;

    // Stitch the next block if we are too close to the end
    while inode_data.len() < byte_offset + 40 {
        let mut next = read_metadata_block(reader, is_le)?;
        inode_data.append(&mut next);
    }

    let data = &inode_data[byte_offset..];
    
    let header = InodeHeader {
        inode_type: read_u16(&data[0..2], is_le),
        permissions: read_u16(&data[2..4], is_le),
        uid: read_u16(&data[4..6], is_le),
        gid: read_u16(&data[6..8], is_le),
        mtime: read_u32(&data[8..12], is_le),
        inode_number: read_u32(&data[12..16], is_le),
    };

    let payload = &data[16..];
    
    let location = match header.inode_type {
        1 => {
            DirectoryLocation {
                path: "".to_string(),
                block_index: read_u32(&payload[0..4], is_le),
                file_size: read_u16(&payload[8..10], is_le) as u32,
                block_offset: read_u16(&payload[10..12], is_le),
            }
        }
        8 => {
            DirectoryLocation {
                path: "".to_string(),
                file_size: read_u32(&payload[4..8], is_le),
                block_index: read_u32(&payload[8..12], is_le),
                block_offset: read_u16(&payload[18..20], is_le),
            }
        }
        _ => return Err(Error::new(ErrorKind::Unsupported, "Expected Directory Inode")),
    };

    Ok((header, location))
}


pub fn read_file_inode<R: Read + Seek>(
    reader: &mut R, superblock: &Superblock, block_index: u32, block_offset: u16, is_le: bool,
) -> Result<FileBlueprint> {
    
    let physical_offset = superblock.inode_table_start + (block_index as u64);
    reader.seek(SeekFrom::Start(physical_offset))?;
    
    let mut inode_data = read_metadata_block(reader, is_le)?;

    // Ensure we have at least 56 bytes to parse an ExtendedFile payload without panicking
    while inode_data.len() < (block_offset as usize) + 56 {
        let mut next = read_metadata_block(reader, is_le)?;
        inode_data.append(&mut next);
    }

    let data = &inode_data[block_offset as usize ..];
    let inode_type = read_u16(&data[0..2], is_le);
    let payload = &data[16..];

    // Added Support for ExtendedFiles (Type 9)
    let (blocks_start, frag_index, block_offset_frag, file_size, block_sizes_start) = match inode_type {
        2 => { // Basic File
            (
                read_u32(&payload[0..4], is_le) as u64,
                read_u32(&payload[4..8], is_le),
                read_u32(&payload[8..12], is_le),
                read_u32(&payload[12..16], is_le),
                16
            )
        }
        9 => { // Extended File
            (
                read_u64(&payload[0..8], is_le),
                read_u32(&payload[28..32], is_le),
                read_u32(&payload[32..36], is_le),
                read_u64(&payload[8..16], is_le) as u32, 
                40
            )
        }
        _ => return Err(Error::new(ErrorKind::Unsupported, format!("Expected File Inode, got: {}", inode_type))),
    };

    let block_count = if frag_index == 0xFFFFFFFF {
        (file_size + superblock.block_size - 1) / superblock.block_size
    } else {
        file_size / superblock.block_size
    };

    // Huge files (1GB+) have thousands of block sizes. We might need to stitch multiple 
    // metadata blocks together just to read the block sizes array
    let required_total_size = block_offset as usize + 16 + block_sizes_start + (block_count as usize * 4);
    
    while inode_data.len() < required_total_size {
        let mut next = read_metadata_block(reader, is_le)?;
        inode_data.append(&mut next);
    }

    // Re-slice because appending may have reallocated the vector
    let data = &inode_data[block_offset as usize ..];
    let payload = &data[16..];

    let mut block_sizes = Vec::new();
    let mut cursor = block_sizes_start;
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


// -- Search logic

pub fn find_files<R: Read + Seek>(
    reader: &mut R,
    superblock: &Superblock,
    root_loc: DirectoryLocation,
    search_list: &mut HashSet<String>,
    path_filter: &str, 
    is_le: bool,
) -> Result<Vec<FileLocation>> {
    
    let mut found_files = Vec::new();
    let mut dir_queue = vec![root_loc];

    while let Some(dir_loc) = dir_queue.pop() {
        if search_list.is_empty() { break; }

        let physical_offset = superblock.directory_table_start + (dir_loc.block_index as u64);
        reader.seek(SeekFrom::Start(physical_offset))?;

        let mut dir_data = Vec::new();
        let target_size = dir_loc.block_offset as usize + dir_loc.file_size as usize - 3; 
        
        while dir_data.len() < target_size {
            let mut block = read_metadata_block(reader, is_le)?;
            dir_data.append(&mut block);
        }

        let payload = &dir_data[dir_loc.block_offset as usize .. target_size];
        let mut cursor = 0;
        let mut sub_dirs = Vec::new(); 

        while cursor + 12 <= payload.len() && !search_list.is_empty() {
            let count = read_u32(&payload[cursor..cursor+4], is_le) + 1; 
            let start = read_u32(&payload[cursor+4..cursor+8], is_le); 
            cursor += 12;

            for _ in 0..count {
                if cursor + 8 > payload.len() { break; } 
                
                let offset = read_u16(&payload[cursor..cursor+2], is_le); 
                let file_type = read_u16(&payload[cursor+4..cursor+6], is_le); 
                let name_size = read_u16(&payload[cursor+6..cursor+8], is_le) as usize + 1;
                cursor += 8;

                if cursor + name_size > payload.len() { break; } 
                
                let name_bytes = &payload[cursor..cursor+name_size];
                let name = String::from_utf8_lossy(name_bytes).to_string();
                cursor += name_size;

                // Build the full path
                let full_path = if dir_loc.path.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", dir_loc.path, name)
                };

                if file_type == 1 {
                    sub_dirs.push((start, offset, full_path)); // Save path for children
                } else if file_type == 2 {
                    // We only pop the list if the filename matches AND the path matches our kernel filter!
                    if search_list.contains(&name) && (path_filter.is_empty() || full_path.contains(path_filter)) {
                        found_files.push(FileLocation {
                            path: full_path, // Save the full path!
                            block_index: start,
                            block_offset: offset,
                        });
                        
                        search_list.remove(&name);
                        if search_list.is_empty() { break; }
                    }
                }
            }
        }

        // Add subdirectories to the queue
        for (start, offset, sub_path) in sub_dirs {
            if search_list.is_empty() { break; }
            let child_dir = read_dir_inode(reader, superblock, start, offset, sub_path, is_le)?;
            dir_queue.push(child_dir);
        }
    }

    Ok(found_files)
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



pub fn extract_file_from_squashfs<R: Read + Seek>(mut reader: R, target_filename: &str
) -> StdResult<(String, Vec<u8>), String> {
    /*
    - Read the Superblock to map the disk
    - Follow the 64-bit pointer into the Inode Table to find the Root Directory
    - Sweep through the compressed Directory Table 
    - Grab file (eg vfat.ko.zst) cooridnates
    - Jump back to the Inode Table to read the file's Blueprint
    - Jump to the raw Data Blocks, unpack, and reassembled the driver in RAM
    */

    // Map the disk
    let superblock = read_superblock(&mut reader).map_err(|e| e.to_string())?;
    
    let is_le = superblock.magic == SQUASHFS_MAGIC;

    // Build the search list with compressed variants
    let mut search_list = HashSet::from([
        target_filename.to_string(),
        format!("{}.xz", target_filename),
        format!("{}.zst", target_filename),
        format!("{}.gz", target_filename),
    ]);

    // Get the Root Directory
    let (_, root_loc) = read_root_inode(&mut reader, &superblock, is_le)
        .map_err(|e| e.to_string())?;

    // Sweep the entire OS tree for the driver (passing "" for no path filter)
    let found_files = find_files(&mut reader, &superblock, root_loc, &mut search_list, "", is_le)
        .map_err(|e| e.to_string())?;

    if let Some(target) = found_files.into_iter().next() {
        
        let blueprint = read_file_inode(
            &mut reader, &superblock, target.block_index, target.block_offset, is_le
        ).map_err(|e| e.to_string())?;
        
        let data = extract_file_data(&mut reader, &superblock, &blueprint, is_le)
            .map_err(|e| e.to_string())?;

        return Ok((target.path, data));
    }

    Err(format!("{} not found in SquashFS!", target_filename))
}


#[pyfunction]
#[pyo3(name = "extract_file_from_squashfs")]
pub fn extract_file_from_squashfs_py(squashfs_path: &str, target_filename: &str) -> PyResult<(String, Vec<u8>)> {
    
    let file = File::open(squashfs_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open SquashFS file: {}", e)))?;

    extract_file_from_squashfs(file, target_filename)
        .map_err(|e| PyRuntimeError::new_err(format!("SquashFS Extraction Failed: {}", e)))
}