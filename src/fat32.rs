use std::io::{Write, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};


pub fn format_fat32<T: Write + Seek>(
    drive: &mut T,
    volume_size: u64,
    volume_label: &str,
    start_lba: u32,
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