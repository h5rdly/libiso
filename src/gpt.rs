use std::io::{Write, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};


// ── MINIMAL SOFTWARE CRC32 (ISO-HDLC) ──
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFFFFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB88320 & mask);
        }
    }
    crc ^ 0xFFFFFFFF
}


// ── UUID GENERATOR ──
pub fn pseudo_uuid() -> [u8; 16] {

    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let s = t.as_secs();
    let n = t.subsec_nanos();
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&s.to_le_bytes());
    bytes[8..12].copy_from_slice(&n.to_le_bytes());
    let mix = (s as u32) ^ n;
    bytes[12..16].copy_from_slice(&mix.to_le_bytes());
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // Version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // Variant 1
    bytes
}


// ── BARE METAL PARTITIONER ──

pub fn write_partition_table<T: Write + Seek>(
    drive: &mut T,
    total_sectors: u64,
    is_gpt: bool,
    part1: (u64, u64), // (start_lba, size_sectors)
    efi_part: Option<(u64, u64)>,
    linux_part: Option<(u64, u64)>,
    is_ntfs: bool,
) -> Result<(), String> {
    
    let mut mbr = [0u8; 512];
    mbr[510] = 0x55;
    mbr[511] = 0xAA;

    if is_gpt {
        // 1. Protective MBR (LBA 0)
        let prot_size = if total_sectors > 0xFFFFFFFF { 0xFFFFFFFF } else { (total_sectors - 1) as u32 };
        mbr[446] = 0x00; // Boot indicator
        mbr[447..450].copy_from_slice(&[0x00, 0x02, 0x00]); // Start CHS
        mbr[450] = 0xEE; // Type: GPT Protective
        mbr[451..454].copy_from_slice(&[0xFF, 0xFF, 0xFF]); // End CHS
        mbr[454..458].copy_from_slice(&1u32.to_le_bytes()); // Start LBA
        mbr[458..462].copy_from_slice(&prot_size.to_le_bytes()); // Sector Count
        
        drive.seek(SeekFrom::Start(0)).unwrap();
        drive.write_all(&mbr).unwrap();

        // 2. Build Partition Entry Array (128 entries * 128 bytes = 16384 bytes)
        let mut entries = vec![0u8; 16384];
        let disk_guid = pseudo_uuid();
        
        let mut add_gpt_entry = |idx: usize, type_guid: [u8; 16], start: u64, size: u64, name: &str| {
            let offset = idx * 128;
            entries[offset .. offset + 16].copy_from_slice(&type_guid);
            entries[offset + 16 .. offset + 32].copy_from_slice(&pseudo_uuid());
            entries[offset + 32 .. offset + 40].copy_from_slice(&start.to_le_bytes());
            entries[offset + 40 .. offset + 48].copy_from_slice(&(start + size - 1).to_le_bytes());
            // 48..56 is Attributes (0)
            let name_utf16: Vec<u16> = name.encode_utf16().take(36).collect();
            for (i, &c) in name_utf16.iter().enumerate() {
                entries[offset + 56 + i * 2 .. offset + 58 + i * 2].copy_from_slice(&c.to_le_bytes());
            }
        };

        // GUIDs
        let guid_data = [0xA2, 0xA0, 0xD0, 0xEB, 0xE5, 0xB9, 0x33, 0x44, 0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26, 0x99, 0xC7];
        let guid_efi = [0x28, 0x73, 0x2A, 0xC1, 0x1F, 0xF8, 0xD2, 0x11, 0xBA, 0x4B, 0x00, 0xA0, 0xC9, 0x3E, 0xC9, 0x3B];
        let guid_linux = [0xAF, 0x3D, 0xC6, 0x0F, 0x83, 0x84, 0x72, 0x47, 0x8E, 0x79, 0x3D, 0x69, 0xD8, 0x47, 0x7D, 0xE4];

        add_gpt_entry(0, guid_data, part1.0, part1.1, "Primary Data");
        if let Some(efi) = efi_part { add_gpt_entry(1, guid_efi, efi.0, efi.1, "EFI System"); }
        if let Some(lin) = linux_part { add_gpt_entry(2, guid_linux, lin.0, lin.1, "Persistence"); }

        let entries_crc = crc32(&entries);

        // 3. Build Header Function
        let build_header = |my_lba: u64, alt_lba: u64, array_lba: u64| -> [u8; 512] {
            let mut hdr = [0u8; 512];
            hdr[0..8].copy_from_slice(b"EFI PART");
            hdr[8..12].copy_from_slice(&0x00010000u32.to_le_bytes());
            hdr[12..16].copy_from_slice(&92u32.to_le_bytes());
            // 16..20 is CRC32, leave 0 for now
            hdr[24..32].copy_from_slice(&my_lba.to_le_bytes());
            hdr[32..40].copy_from_slice(&alt_lba.to_le_bytes());
            hdr[40..48].copy_from_slice(&34u64.to_le_bytes()); // First Usable
            hdr[48..56].copy_from_slice(&(total_sectors - 34).to_le_bytes()); // Last Usable
            hdr[56..72].copy_from_slice(&disk_guid);
            hdr[72..80].copy_from_slice(&array_lba.to_le_bytes());
            hdr[80..84].copy_from_slice(&128u32.to_le_bytes()); // Num entries
            hdr[84..88].copy_from_slice(&128u32.to_le_bytes()); // Entry size
            hdr[88..92].copy_from_slice(&entries_crc.to_le_bytes());
            
            let hdr_crc = crc32(&hdr[0..92]);
            hdr[16..20].copy_from_slice(&hdr_crc.to_le_bytes());
            hdr
        };

        let primary_hdr = build_header(1, total_sectors - 1, 2);
        let backup_hdr = build_header(total_sectors - 1, 1, total_sectors - 33);

        // Write Primary GPT (LBA 1, then LBA 2-33)
        drive.seek(SeekFrom::Start(512)).unwrap();
        drive.write_all(&primary_hdr).unwrap();
        drive.write_all(&entries).unwrap();

        // Write Backup GPT (LBA total-33, then LBA total-1)
        drive.seek(SeekFrom::Start((total_sectors - 33) * 512)).unwrap();
        drive.write_all(&entries).unwrap();
        drive.write_all(&backup_hdr).unwrap();

    } else {
        // MBR Mode
        let mut add_mbr_entry = |idx: usize, type_byte: u8, start: u64, size: u64, bootable: bool| {
            let offset = 446 + (idx * 16);
            mbr[offset] = if bootable { 0x80 } else { 0x00 };
            mbr[offset + 1 .. offset + 4].copy_from_slice(&[0xFE, 0xFF, 0xFF]); // Dummy CHS
            mbr[offset + 4] = type_byte;
            mbr[offset + 5 .. offset + 8].copy_from_slice(&[0xFE, 0xFF, 0xFF]); // Dummy CHS
            mbr[offset + 8 .. offset + 12].copy_from_slice(&(start as u32).to_le_bytes());
            mbr[offset + 12 .. offset + 16].copy_from_slice(&(size as u32).to_le_bytes());
        };

        let p1_type = if is_ntfs { 0x07 } else { 0x0C }; // 0x0C = FAT32 LBA
        add_mbr_entry(0, p1_type, part1.0, part1.1, true);
        if let Some(efi) = efi_part { add_mbr_entry(1, 0xEF, efi.0, efi.1, false); }
        if let Some(lin) = linux_part { add_mbr_entry(2, 0x83, lin.0, lin.1, false); }

        drive.seek(SeekFrom::Start(0)).unwrap();
        drive.write_all(&mbr).unwrap();
    }
    
    Ok(())
}