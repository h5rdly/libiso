use std::sync::Arc;
use std::io::{Cursor, Read, Write, Seek, SeekFrom};

use hadris_iso::write::{IsoImageWriter, InputFiles, File as IsoFile};
use hadris_iso::write::options::{FormatOptions, CreationFeatures, BaseIsoLevel};
use hadris_iso::read::PathSeparator;

use pyo3::prelude::*;


#[pyfunction]
#[pyo3(signature = (volume_name, files, is_isohybrid, dummy_file_size_mb=1))]
pub fn create_mock_iso(
    volume_name: String, files: Vec<String>, is_isohybrid: bool, dummy_file_size_mb: usize
) -> PyResult<Vec<u8>> {

    let mut iso_files = Vec::new();
    
    let file_content = vec![0u8; dummy_file_size_mb * 1024 * 1024];
    
    for name in &files {
        iso_files.push(IsoFile::File {
            name: Arc::new(name.clone()),
            contents: file_content.clone(),
        });
    }

    let input_files = InputFiles {
        path_separator: PathSeparator::ForwardSlash,
        files: iso_files,
    };

    let mut features = CreationFeatures::default();
    features.filenames = BaseIsoLevel::Level2 {
        supports_lowercase: true,
        supports_rrip: false,
    };

    let options = FormatOptions {
        volume_name,
        system_id: None,
        volume_set_id: None,
        publisher_id: None,
        preparer_id: None,
        application_id: None,
        sector_size: 2048,
        features,
        path_separator: PathSeparator::ForwardSlash,
        strict_charset: false, 
    };

    // Pre-allocate and zero-fill the buffer.
    // The ISO writer expects a physical-disk-like canvas to seek around on.
    let total_size = (files.len() * dummy_file_size_mb * 1024 * 1024) + (4 * 1024 * 1024);
    let mut buffer = Cursor::new(vec![0u8; total_size]);
    
    IsoImageWriter::format_new(&mut buffer, input_files, options)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("ISO creation failed: {:?}", e)))?;

    let mut bytes = buffer.into_inner();

    // Inject the ISOHybrid MBR signature at the end of Sector 0
    if is_isohybrid && bytes.len() >= 512 {
        bytes[510] = 0x55;
        bytes[511] = 0xAA;
    }

    Ok(bytes)
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
    let (tx, _rx) = kanal::unbounded();
    let fake_cap = drive.fake_capacity;
    
    // Dereference `drive` to access the Rust struct inside the Python wrapper
    crate::verify::verify_hardware_capacity(&mut *drive, fake_cap, &tx, |_| Ok(()))
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