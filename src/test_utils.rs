use std::sync::Arc;
use std::io::Cursor;

use hadris_iso::write::{IsoImageWriter, InputFiles, File as IsoFile};
use hadris_iso::write::options::{FormatOptions, CreationFeatures, BaseIsoLevel};
use hadris_iso::read::PathSeparator;

use pyo3::prelude::*;


#[pyfunction]
pub fn create_mock_iso(volume_name: String, files: Vec<String>, is_isohybrid: bool) -> PyResult<Vec<u8>> {
    let mut iso_files = Vec::new();
    
    for name in files {
        iso_files.push(IsoFile::File {
            name: Arc::new(name),
            contents: b"mock_data".to_vec(),
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
        system_id: None, volume_set_id: None, publisher_id: None,
        preparer_id: None, application_id: None,
        sector_size: 2048,
        path_separator: PathSeparator::ForwardSlash,
        features,
    };

    let mut buffer = Cursor::new(vec![0u8; 2 * 1024 * 1024]);
    
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