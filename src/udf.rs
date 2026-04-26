// UDF (ECMA-167) reading logic, based on hadris-udf - 
// https://github.com/hxyulin/hadris/tree/main/crates/hadris-udf

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use bytemuck::{Pod, Zeroable};


pub const SECTOR_SIZE: usize = 2048;


#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct DescriptorTag {
    pub tag_identifier: u16,
    pub descriptor_version: u16,
    pub tag_checksum: u8,
    pub reserved: u8,
    pub tag_serial_number: u16,
    pub descriptor_crc: u16,
    pub descriptor_crc_length: u16,
    pub tag_location: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct ExtentDescriptor {
    pub length: u32,
    pub location: u32,
}


#[repr(C)]
#[derive(Debug, Clone, Copy, Default, Zeroable, Pod)]
pub struct LongAllocationDescriptor {
    pub extent_length: u32,
    pub logical_block_num: u32,
    pub partition_ref_num: u16,
    pub impl_use: [u8; 6],
}


#[repr(C)]
#[derive(Debug, Clone, Copy, Default, Zeroable, Pod)]
pub struct ShortAllocationDescriptor {
    pub extent_length: u32,
    pub extent_position: u32,
}


#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct AnchorVolumeDescriptorPointer {
    pub tag: DescriptorTag,
    pub main_vds_extent: ExtentDescriptor,
    pub reserve_vds_extent: ExtentDescriptor,
    pub reserved: [u8; 480],
}


#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct FileEntry {
    pub tag: DescriptorTag,
    pub icb_tag: [u8; 20], // Simplified ICB tag since we only need the flags
    pub uid: u32,
    pub gid: u32,
    pub permissions: u32,
    pub file_link_count: u16,
    pub record_format: u8,
    pub record_display_attributes: u8,
    pub record_length: u32,
    pub information_length: u64,
    pub logical_blocks_recorded: u64,
    pub access_time: [u8; 12],
    pub modification_time: [u8; 12],
    pub attribute_time: [u8; 12],
    pub checkpoint: u32,
    pub extended_attribute_icb: LongAllocationDescriptor,
    pub implementation_identifier: [u8; 32],
    pub unique_id: u64,
    pub extended_attributes_length: u32,
    pub allocation_descriptors_length: u32,
}

impl FileEntry {
    pub const BASE_SIZE: usize = 176;
    
    pub fn allocation_type(&self) -> u8 {
        // The allocation type is in the lowest 3 bits of the flags field of the ICB tag
        // Flags are at offset 18-19 in the ICB tag
        let flags = u16::from_le_bytes([self.icb_tag[18], self.icb_tag[19]]);
        (flags & 0x07) as u8
    }
}


// --- MISSING DESCRIPTORS ---

#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct PartitionDescriptor {
    pub tag: DescriptorTag,
    pub vds_number: u32,
    pub partition_flags: u16,
    pub partition_number: u16,
    pub partition_contents: [u8; 32],
    pub partition_contents_use: [u8; 128],
    pub access_type: u32,
    pub partition_starting_location: u32,
    pub partition_length: u32,
    pub implementation_identifier: [u8; 32],
    pub implementation_use: [u8; 128],
    pub reserved: [u8; 156],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct LogicalVolumeDescriptor {
    pub tag: DescriptorTag,
    pub vds_number: u32,
    pub descriptor_char_set: [u8; 64],
    pub logical_volume_identifier: [u8; 128],
    pub logical_block_size: u32,
    pub domain_identifier: [u8; 32],
    pub logical_volume_contents_use: [u8; 16],
    pub map_table_length: u32,
    pub num_partition_maps: u32,
    pub implementation_identifier: [u8; 32],
    pub implementation_use: [u8; 128],
    pub integrity_sequence_extent: ExtentDescriptor,
    pub partition_maps: [u8; 72],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct FileSetDescriptor {
    pub tag: DescriptorTag,
    pub recording_date_time: [u8; 12],
    pub interchange_level: u16,
    pub max_interchange_level: u16,
    pub character_set_list: u32,
    pub max_character_set_list: u32,
    pub file_set_number: u32,
    pub file_set_desc_number: u32,
    pub logical_volume_id_char_set: [u8; 64],
    pub logical_volume_identifier: [u8; 128],
    pub file_set_char_set: [u8; 64],
    pub file_set_identifier: [u8; 32],
    pub copyright_file_identifier: [u8; 32],
    pub abstract_file_identifier: [u8; 32],
    pub root_directory_icb: LongAllocationDescriptor,
    pub domain_identifier: [u8; 32],
    pub next_extent: LongAllocationDescriptor,
    pub system_stream_directory_icb: LongAllocationDescriptor,
    pub reserved: [u8; 32],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct ExtendedFileEntry {
    pub tag: DescriptorTag,
    pub icb_tag: [u8; 20],
    pub uid: u32,
    pub gid: u32,
    pub permissions: u32,
    pub file_link_count: u16,
    pub record_format: u8,
    pub record_display_attributes: u8,
    pub record_length: u32,
    pub information_length: u64,
    pub object_size: u64,
    pub logical_blocks_recorded: u64,
    pub access_time: [u8; 12],
    pub modification_time: [u8; 12],
    pub creation_time: [u8; 12],
    pub attribute_time: [u8; 12],
    pub checkpoint: u32,
    pub reserved: u32,
    pub extended_attribute_icb: LongAllocationDescriptor,
    pub stream_directory_icb: LongAllocationDescriptor,
    pub implementation_identifier: [u8; 32],
    pub unique_id: u64,
    pub extended_attributes_length: u32,
    pub allocation_descriptors_length: u32,
}

impl ExtendedFileEntry {
    pub const BASE_SIZE: usize = 216;
    
    pub fn allocation_type(&self) -> u8 {
        let flags = u16::from_le_bytes([self.icb_tag[18], self.icb_tag[19]]);
        (flags & 0x07) as u8
    }
}


#[derive(Clone, Debug)]
pub struct UdfDirEntry {
    pub name: String,
    pub is_directory: bool,
    pub icb: LongAllocationDescriptor,
}


#[derive(Clone, Debug)]
pub struct UdfContext {
    pub partition_start: u32,
    pub volume_id: String,
    pub root_icb: LongAllocationDescriptor,
}


pub fn mount_udf(file: &mut File) -> Result<UdfContext, String> {

    // Read AVDP at sector 256
    file.seek(SeekFrom::Start(256 * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
    let mut buf = [0u8; SECTOR_SIZE];
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;

    let avdp_tag: DescriptorTag = bytemuck::pod_read_unaligned(&buf[..16]);
    if avdp_tag.tag_identifier != 2 {
        return Err("AVDP not found at sector 256".to_string());
    }
    let avdp: AnchorVolumeDescriptorPointer = bytemuck::pod_read_unaligned(&buf[..512]);

    // Scan Main VDS
    let vds_loc = avdp.main_vds_extent.location as u64;
    let vds_len = avdp.main_vds_extent.length as u64 / SECTOR_SIZE as u64;

    let mut opt_partition = None;
    let mut opt_lvd = None;

    for i in 0..vds_len {
        file.seek(SeekFrom::Start((vds_loc + i) * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
        file.read_exact(&mut buf).map_err(|e| e.to_string())?;

        let tag: DescriptorTag = bytemuck::pod_read_unaligned(&buf[..16]);
        match tag.tag_identifier {
            5 => opt_partition = Some(bytemuck::pod_read_unaligned::<PartitionDescriptor>(&buf[..512])),
            6 => opt_lvd = Some(bytemuck::pod_read_unaligned::<LogicalVolumeDescriptor>(&buf[..512])),
            8 => break, // TerminatingDescriptor
            _ => {}
        }
    }

    let partition = opt_partition.ok_or("Partition Descriptor not found")?;
    let lvd = opt_lvd.ok_or("Logical Volume Descriptor not found")?;

    // Extract volume ID
    let vol_id = decode_dstring(&lvd.logical_volume_identifier);

    // Read File Set Descriptor (FSD)
    let fsd_loc = lvd.logical_volume_contents_use; 
    let fsd_icb: LongAllocationDescriptor = bytemuck::pod_read_unaligned(&fsd_loc);
    
    let fsd_sector = partition.partition_starting_location as u64 + fsd_icb.logical_block_num as u64;
    file.seek(SeekFrom::Start(fsd_sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;

    let fsd: FileSetDescriptor = bytemuck::pod_read_unaligned(&buf[..512]);

    Ok(UdfContext {
        partition_start: partition.partition_starting_location,
        volume_id: vol_id,
        root_icb: fsd.root_directory_icb,
    })
}


fn decode_dstring(data: &[u8]) -> String {

    if data.is_empty() { return String::new(); }
    let compression_id = data[0];
    let len = data[data.len() - 1] as usize;
    if len == 0 || len > data.len() - 1 { return String::new(); }
    
    let content = &data[1..=len.min(data.len() - 2)];
    match compression_id {
        8 => String::from_utf8_lossy(content).into_owned(),
        16 => {
            let chars: Vec<u16> = content.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
            String::from_utf16_lossy(&chars)
        }
        _ => String::new()
    }
}


// Filenames use a different string format than the Volume Label
fn decode_filename(data: &[u8]) -> String {
    if data.is_empty() { return String::new(); }
    let compression_id = data[0];
    let content = &data[1..]; // No trailing length byte here
    
    match compression_id {
        8 => String::from_utf8_lossy(content).into_owned(),
        16 => {
            let chars: Vec<u16> = content.chunks_exact(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
            String::from_utf16_lossy(&chars)
        }
        _ => String::new()
    }
}


pub fn read_directory(file: &mut File, partition_start: u32, icb: &LongAllocationDescriptor) -> Result<Vec<UdfDirEntry>, String> {
    
    let sector = partition_start as u64 + icb.logical_block_num as u64;
    file.seek(SeekFrom::Start(sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
    
    let mut buffer = [0u8; SECTOR_SIZE];
    file.read_exact(&mut buffer).map_err(|e| e.to_string())?;

    let tag: DescriptorTag = bytemuck::pod_read_unaligned(&buffer[..16]);
    
    let (allocation_type, ad_offset, ad_length) = match tag.tag_identifier {
        261 => { // FileEntry
            let fe: FileEntry = bytemuck::pod_read_unaligned(&buffer[..FileEntry::BASE_SIZE]);
            (fe.allocation_type(), FileEntry::BASE_SIZE + fe.extended_attributes_length as usize, fe.allocation_descriptors_length as usize)
        },
        266 => { // ExtendedFileEntry
            let efe: ExtendedFileEntry = bytemuck::pod_read_unaligned(&buffer[..ExtendedFileEntry::BASE_SIZE]);
            (efe.allocation_type(), ExtendedFileEntry::BASE_SIZE + efe.extended_attributes_length as usize, efe.allocation_descriptors_length as usize)
        }
        _ => return Err(format!("Invalid ICB tag id: {}", tag.tag_identifier)),
    };

    // Protect against out-of-bounds if AD data spills over 1 sector
    let safe_ad_length = ad_length.min(SECTOR_SIZE.saturating_sub(ad_offset));
    let ad_data = &buffer[ad_offset..ad_offset + safe_ad_length];
    let mut entries = Vec::new();

    match allocation_type {
        3 => { parse_fids(ad_data, &mut entries)?; }, // 3 = Embedded
        0 => { // 0 = Short
            for chunk in ad_data.chunks(8) {
                if chunk.len() < 8 { break; }
                let sad: ShortAllocationDescriptor = bytemuck::pod_read_unaligned(chunk);
                let len = sad.extent_length & 0x3FFFFFFF; // Mask out the type bits!
                if len == 0 { break; }

                let ext_sector = partition_start as u64 + sad.extent_position as u64;
                file.seek(SeekFrom::Start(ext_sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
                let mut ext_buf = vec![0u8; len as usize];
                file.read_exact(&mut ext_buf).map_err(|e| e.to_string())?;

                parse_fids(&ext_buf, &mut entries)?;
            }
        },
        1 => { // 1 = Long (WINDOWS 10/11 USES THIS!)
            for chunk in ad_data.chunks(16) {
                if chunk.len() < 16 { break; }
                let lad: LongAllocationDescriptor = bytemuck::pod_read_unaligned(chunk);
                let len = lad.extent_length & 0x3FFFFFFF; // Mask out the type bits!
                if len == 0 { break; }

                let ext_sector = partition_start as u64 + lad.logical_block_num as u64;
                file.seek(SeekFrom::Start(ext_sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
                let mut ext_buf = vec![0u8; len as usize];
                file.read_exact(&mut ext_buf).map_err(|e| e.to_string())?;

                parse_fids(&ext_buf, &mut entries)?;
            }
        },
        _ => return Err(format!("Unsupported directory allocation type: {}", allocation_type))
    }
    Ok(entries)
}


fn parse_fids(data: &[u8], entries: &mut Vec<UdfDirEntry>) -> Result<(), String> {
    
    let base_size = 38; // FileIdentifierDescriptor base size
    let mut offset = 0;
    while offset < data.len() {
        if offset + base_size > data.len() { break; }
        
        let fid_base = &data[offset..offset + base_size];
        let tag: DescriptorTag = bytemuck::pod_read_unaligned(&fid_base[0..16]);
        
        // Tag 257 is FileIdentifierDescriptor. If it's 0, we hit the padding at the end of the sector.
        if tag.tag_identifier != 257 { break; } 

        let chars = fid_base[18];
        let file_identifier_length = fid_base[19] as usize;
        let icb: LongAllocationDescriptor = bytemuck::pod_read_unaligned(&fid_base[20..36]);
        let implementation_use_length = u16::from_le_bytes([fid_base[36], fid_base[37]]) as usize;

        let total_size = (base_size + implementation_use_length + file_identifier_length + 3) & !3;

        let is_directory = (chars & 0x02) != 0;
        let is_deleted = (chars & 0x04) != 0;
        let is_parent = (chars & 0x08) != 0;

        if !is_deleted {
            let name_start = offset + base_size + implementation_use_length;
            let name_end = name_start + file_identifier_length;
            let name_data = if name_end <= data.len() { &data[name_start..name_end] } else { &[] };

            let name = if is_parent {
                "..".to_string()
            } else if name_data.is_empty() {
                "".to_string()
            } else {
                decode_filename(name_data)
            };

            entries.push(UdfDirEntry { name, is_directory, icb });
        }
        offset += total_size;
    }
    Ok(())
}


pub fn find_udf_entry(file: &mut File, partition_start: u32, root_icb: &LongAllocationDescriptor, path: &str) -> Option<UdfDirEntry> {
    
    let mut current_icb = *root_icb;
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    
    for (i, part) in parts.iter().enumerate() {
        let entries = read_directory(file, partition_start, &current_icb).ok()?;
        let mut found_entry = None;
        for entry in entries {
            let clean_name = entry.name.split(';').next().unwrap_or(&entry.name);
            if clean_name.eq_ignore_ascii_case(part) {
                found_entry = Some(entry);
                break;
            }
        }
        
        let entry = found_entry?;
        if i == parts.len() - 1 {
            return Some(entry);
        } else if entry.is_directory {
            current_icb = entry.icb;
        } else {
            return None;
        }
    }
    None
}


pub fn stream_file_data<F>(
    iso_file: &mut File,
    partition_start: u32,
    entry: &UdfDirEntry,
    chunk_buf: &mut [u8],
    mut on_chunk: F,
) -> Result<(), String>
where
    F: FnMut(&[u8]) -> Result<(), String>,
{
    let sector = partition_start as u64 + entry.icb.logical_block_num as u64;
    iso_file.seek(SeekFrom::Start(sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
    
    let mut buffer = [0u8; SECTOR_SIZE];
    iso_file.read_exact(&mut buffer).map_err(|e| e.to_string())?;
    
    let fe: FileEntry = bytemuck::pod_read_unaligned(&buffer[..FileEntry::BASE_SIZE]);
    
    let ad_offset = FileEntry::BASE_SIZE + fe.extended_attributes_length as usize;
    let ad_length = fe.allocation_descriptors_length as usize;
    
    // Protect against out-of-bounds
    let safe_ad_length = ad_length.min(SECTOR_SIZE.saturating_sub(ad_offset));
    let ad_data = &buffer[ad_offset..ad_offset + safe_ad_length];
    
    match fe.allocation_type() {
        0 => { // Short Allocation
            for chunk in ad_data.chunks(8) {
                if chunk.len() < 8 { break; }
                let sad: ShortAllocationDescriptor = bytemuck::pod_read_unaligned(chunk);
                let extent_len = (sad.extent_length & 0x3FFFFFFF) as u64;
                if extent_len == 0 { break; }
                
                let ext_sector = partition_start as u64 + sad.extent_position as u64;
                iso_file.seek(SeekFrom::Start(ext_sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
                
                let mut extent_offset = 0u64;
                while extent_offset < extent_len {
                    let read_size = (extent_len - extent_offset).min(chunk_buf.len() as u64) as usize;
                    iso_file.read_exact(&mut chunk_buf[..read_size]).map_err(|e| e.to_string())?;
                    on_chunk(&chunk_buf[..read_size])?;
                    extent_offset += read_size as u64;
                }
            }
        },
        1 => { // Long Allocation
            for chunk in ad_data.chunks(16) {
                if chunk.len() < 16 { break; }
                let lad: LongAllocationDescriptor = bytemuck::pod_read_unaligned(chunk);
                let extent_len = (lad.extent_length & 0x3FFFFFFF) as u64;
                if extent_len == 0 { break; }
                
                let ext_sector = partition_start as u64 + lad.logical_block_num as u64;
                iso_file.seek(SeekFrom::Start(ext_sector * SECTOR_SIZE as u64)).map_err(|e| e.to_string())?;
                
                let mut extent_offset = 0u64;
                while extent_offset < extent_len {
                    let read_size = (extent_len - extent_offset).min(chunk_buf.len() as u64) as usize;
                    iso_file.read_exact(&mut chunk_buf[..read_size]).map_err(|e| e.to_string())?;
                    on_chunk(&chunk_buf[..read_size])?;
                    extent_offset += read_size as u64;
                }
            }
        },
        3 => { // Embedded Allocation
            let len = (fe.information_length as usize).min(ad_data.len());
            on_chunk(&ad_data[..len])?;
        },
        _ => return Err(format!("Unsupported UDF Allocation Type: {}", fe.allocation_type())),
    }
    Ok(())
}


pub fn read_file_bytes(iso_file: &mut File, partition_start: u32, entry: &UdfDirEntry) -> Result<Vec<u8>, String> {
    let mut data = Vec::new();
    let mut chunk_buf = vec![0u8; 100 * 1024]; 
    stream_file_data(iso_file, partition_start, entry, &mut chunk_buf, |chunk| {
        data.extend_from_slice(chunk);
        Ok(())
    })?;
    Ok(data)
}