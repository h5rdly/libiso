use std::{
    fs::File, sync::{mpsc}, io::{Read, Write, Seek, SeekFrom}
};

use sha2::{Sha256, Digest};

use pyo3::prelude::*;

use crate::verify;


#[pyfunction]
#[pyo3(signature = (volume_name, files, is_isohybrid, dummy_file_size_mb=1))]
pub fn create_mock_iso(
    volume_name: String, files: Vec<String>, is_isohybrid: bool, dummy_file_size_mb: usize
) -> PyResult<Vec<u8>> {
    
    // ISOs must be a multiple of 2048 bytes. Start with 19 sectors (up to LBA 18 for root dir).
    let mut iso = vec![0u8; 19 * 2048];

    // 1. ISOHybrid MBR signature
    if is_isohybrid {
        iso[510] = 0x55;
        iso[511] = 0xAA;
    }

    // 2. Sector 16: Primary Volume Descriptor (PVD)
    let pvd = 16 * 2048;
    iso[pvd] = 1; // Type: PVD
    iso[pvd + 1..pvd + 6].copy_from_slice(b"CD001");
    iso[pvd + 6] = 1; // Version
    
    let vol_bytes = volume_name.as_bytes();
    let copy_len = vol_bytes.len().min(32);
    iso[pvd + 40..pvd + 40 + copy_len].copy_from_slice(&vol_bytes[..copy_len]);
    for i in copy_len..32 { iso[pvd + 40 + i] = b' '; } // Pad with spaces

    // 3. Sector 17: Terminator
    let term = 17 * 2048;
    iso[term] = 255;
    iso[term + 1..term + 6].copy_from_slice(b"CD001");
    iso[term + 6] = 1;

    // 4. Root Directory Record (Inside PVD at offset 156)
    let root_rec = pvd + 156;
    iso[root_rec] = 34; // Record length
    iso[root_rec + 2..root_rec + 6].copy_from_slice(&18u32.to_le_bytes()); // LBA 18
    iso[root_rec + 10..root_rec + 14].copy_from_slice(&2048u32.to_le_bytes()); // Size 2048
    iso[root_rec + 25] = 0x02; // Directory flag
    iso[root_rec + 32] = 1; // Name length
    iso[root_rec + 33] = 0x00; // '\x00' is the name for the root directory itself

    // 5. Sector 18: Root Directory Data
    let mut dir_cursor = 18 * 2048;
    
    // Add "."
    iso[dir_cursor] = 34;
    iso[dir_cursor + 2..dir_cursor + 6].copy_from_slice(&18u32.to_le_bytes());
    iso[dir_cursor + 25] = 0x02;
    iso[dir_cursor + 32] = 1;
    iso[dir_cursor + 33] = 0x00;
    dir_cursor += 34;

    // Add ".."
    iso[dir_cursor] = 34;
    iso[dir_cursor + 2..dir_cursor + 6].copy_from_slice(&18u32.to_le_bytes());
    iso[dir_cursor + 25] = 0x02;
    iso[dir_cursor + 32] = 1;
    iso[dir_cursor + 33] = 0x01;
    dir_cursor += 34;

    // 6. Add Files
    let mut current_file_lba = 19u32;
    let file_size_bytes = (dummy_file_size_mb * 1024 * 1024) as u32;

    for name in files {
        // Mock the filename to ensure our ISO reader finds it easily
        let mut name_upper = name.to_uppercase().replace("/", "_");
        if !name_upper.contains(';') {
            name_upper.push_str(";1");
        }
        let name_bytes = name_upper.as_bytes();
        let padding = if name_bytes.len() % 2 == 0 { 1 } else { 0 };
        let record_len = 33 + name_bytes.len() + padding;

        iso[dir_cursor] = record_len as u8;
        iso[dir_cursor + 2..dir_cursor + 6].copy_from_slice(&current_file_lba.to_le_bytes());
        iso[dir_cursor + 10..dir_cursor + 14].copy_from_slice(&file_size_bytes.to_le_bytes());
        iso[dir_cursor + 25] = 0x00; // File flag
        iso[dir_cursor + 32] = name_bytes.len() as u8;
        iso[dir_cursor + 33..dir_cursor + 33 + name_bytes.len()].copy_from_slice(name_bytes);
        
        dir_cursor += record_len;

        // Expand the ISO file to accommodate the dummy file data
        let file_sectors = (file_size_bytes as usize + 2047) / 2048;
        iso.resize(iso.len() + (file_sectors * 2048), 0);
        current_file_lba += file_sectors as u32;
    }

    Ok(iso)
}


#[pyclass]
pub struct FakeDrive {
    memory: Vec<u8>,
    #[pyo3(get)]
    pub real_capacity: u64,
    #[pyo3(get)]
    pub fake_capacity: u64,
    #[pyo3(get)]
    pub cursor: u64,
    pub strict_alignment: bool,
}

#[pymethods]
impl FakeDrive {

    #[new]
    #[pyo3(signature = (real_capacity, fake_capacity, strict_alignment=true))]
    pub fn new(real_capacity: u64, fake_capacity: u64, strict_alignment: bool) -> Self {
        Self {
            memory: vec![0; real_capacity as usize],
            real_capacity,
            fake_capacity,
            cursor: 0,
            strict_alignment,
        }
    }

    pub fn read(&mut self, size: usize) -> PyResult<Vec<u8>> {
        let available = (self.fake_capacity - self.cursor) as usize;
        let to_read = std::cmp::min(size, available);
        let mut buf = vec![0u8; to_read];

        for i in 0..to_read {
            let wrapped_pos = ((self.cursor + i as u64) % self.real_capacity) as usize;
            buf[i] = self.memory[wrapped_pos];
        }
        self.cursor += to_read as u64;
        
        Ok(buf) 
    }

    /// Write method exposed directly to Python
    pub fn write(&mut self, buf: &[u8]) -> PyResult<usize> {

        if self.strict_alignment {
            if buf.len() % 512 != 0 || self.cursor % 512 != 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("ERROR_INVALID_PARAMETER: Unbuffered I/O requires 512-byte sector alignment. Attempted to write {} bytes at offset {}.", buf.len(), self.cursor)
                ));
            }
        }

        let available = (self.fake_capacity - self.cursor) as usize;
        let to_write = std::cmp::min(buf.len(), available);

        for i in 0..to_write {
            let wrapped_pos = ((self.cursor + i as u64) % self.real_capacity) as usize;
            self.memory[wrapped_pos] = buf[i];
        }
        self.cursor += to_write as u64;
        Ok(to_write)
    }

    // Seek method mimicking Python's os.SEEK_SET, os.SEEK_CUR, os.SEEK_END
    pub fn seek(&mut self, pos: i64, whence: u8) -> PyResult<u64> {
        let new_pos = match whence {
            0 => pos,                                    // SEEK_SET
            1 => self.cursor as i64 + pos,               // SEEK_CUR
            2 => self.fake_capacity as i64 + pos,        // SEEK_END
            _ => return Err(pyo3::exceptions::PyValueError::new_err("Invalid whence")),
        };

        if new_pos < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err("Invalid seek position"));
        }

        self.cursor = new_pos as u64;
        Ok(self.cursor)
    }

    pub fn tell(&self) -> PyResult<u64> {
        Ok(self.cursor)
    }

    pub fn simulate_os_interference(&mut self) -> PyResult<()> {
        // Windows typically drops this near the start of the disk 
        // after the boot sector (e.g., offset 8192)
        let interference_offset = 8192;
        
        if self.real_capacity < interference_offset + 512 {
            return Ok(()); // Drive too small to care
        }

        let garbage_data = b"SYSTEM_VOLUME_INFORMATION_CORRUPTION_GHOST_DATA";
        
        for (i, &byte) in garbage_data.iter().enumerate() {
            let pos = ((interference_offset + i as u64) % self.real_capacity) as usize;
            self.memory[pos] = byte;
        }
        
        Ok(())
    }
}

// Keeping the standard Rust I/O traits so our verification algorithm can use it
impl Read for FakeDrive {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.read(buf.len()).unwrap();
        let len = bytes.len();
        buf[..len].copy_from_slice(&bytes);
        Ok(len)
    }
}

impl Write for FakeDrive {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write(buf).map_err(|e| std::io::Error::new(
            std::io::ErrorKind::InvalidInput, e.to_string()))
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

impl Seek for FakeDrive {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let (offset, whence) = match pos {
            SeekFrom::Start(p) => (p as i64, 0),
            SeekFrom::Current(p) => (p, 1),
            SeekFrom::End(p) => (p, 2),
        };
        self.seek(offset, whence).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))
    }
}

// Using PyRefMut to borrow the Python object safely without moving it into a thread
#[pyfunction]
pub fn test_verify_fake_drive_sync(mut drive: PyRefMut<'_, FakeDrive>) -> PyResult<()> {
    // Create a dummy channel that we won't actually read from since this is synchronous
    let (tx, _rx) = mpsc::sync_channel(100);
    let fake_cap = drive.fake_capacity;
    
    // Dereference `drive` to access the Rust struct inside the Python wrapper
    verify::verify_hardware_capacity(&mut *drive, fake_cap, &tx, |_| Ok(()))
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))
}


#[pyfunction]
pub fn create_mock_esd() -> PyResult<Vec<u8>> {
    let mut wim = vec![0u8; 152]; // 152-byte WIM Header
    
    // Signature
    wim[0..8].copy_from_slice(b"MSWIM\x00\x00\x00");
    // Header Size
    wim[8..12].copy_from_slice(&152u32.to_le_bytes()); 
    // Image Count
    wim[44..48].copy_from_slice(&1u32.to_le_bytes()); 

    // Build the XML Payload (UTF-16LE with BOM)
    let xml_str = r#"<WIM>
        <IMAGE INDEX="1">
            <TOTALBYTES>987654321</TOTALBYTES>
            <WINDOWS><ARCH>9</ARCH></WINDOWS>
            <DISPLAYNAME>Windows 11 Pro</DISPLAYNAME>
        </IMAGE>
    </WIM>"#;
    
    let mut xml_data = vec![0xFF, 0xFE]; // BOM
    for c in xml_str.encode_utf16() {
        xml_data.extend_from_slice(&c.to_le_bytes());
    }

    let xml_size = xml_data.len() as u64;
    let xml_offset = 1024u64; // Place XML at byte 1024
    
    // Inject XML Resource Header (Offset 72)
    let mut xml_res = [0u8; 24];
    // Write 7-byte size
    xml_res[0..7].copy_from_slice(&xml_size.to_le_bytes()[0..7]);
    xml_res[7] = 0x02; // Flags (Metadata)
    // Write 8-byte offset
    xml_res[8..16].copy_from_slice(&xml_offset.to_le_bytes());
    wim[72..96].copy_from_slice(&xml_res);

    // Solid Resource Header (Appended right after the 152-byte header) 
    let solid_uncompressed_size = 65536u64;
    let comp_format = 3u32; // 3 = LZMS
    let solid_chunk_size = 32768u32;
    
    wim.extend_from_slice(&solid_uncompressed_size.to_le_bytes());
    wim.extend_from_slice(&comp_format.to_le_bytes());
    wim.extend_from_slice(&solid_chunk_size.to_le_bytes());

    // Chunk offsets (2 chunks, 100 bytes each)
    wim.extend_from_slice(&100u64.to_le_bytes());
    wim.extend_from_slice(&200u64.to_le_bytes());

    // Dummy compressed LZMS data (garbage bytes)
    wim.extend(vec![0x42; 200]); 

    // Pad with zeros up to the XML offset, then append the XML
    if wim.len() < xml_offset as usize {
        wim.resize(xml_offset as usize, 0);
    }
    wim.extend(xml_data);

    Ok(wim)
}


#[pyfunction]
#[pyo3(signature = (file_path))]
pub fn hash_sha256(file_path: String) -> PyResult<String> {
    let mut file = File::open(&file_path).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to open file '{}': {}", file_path, e))
    })?;

    let mut hasher = Sha256::new();
    // 64KB buffer for efficient disk streaming
    let mut buffer = vec![0u8; 64 * 1024]; 

    loop {
        let count = file.read(&mut buffer).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to read file: {}", e))
        })?;
        
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }

    let result = hasher.finalize();

    // Manually format each byte as a 2-character lowercase hex string
    let mut hex_string = String::with_capacity(64);
    for byte in result {
        hex_string.push_str(&format!("{:02x}", byte));
    }
    
    Ok(hex_string)
}