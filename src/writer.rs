use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom}; 
use std::path::Path;
use std::thread;

use hadris_part::{GptDisk, GptPartitionEntry, Guid, DiskPartitionScheme};
use hadris_part::scheme_io::DiskPartitionSchemeWriteExt;

use hadris_fat::sync::{FatFs, FatFsWriteExt};
use hadris_fat::sync::dir::FatDir;
use hadris_fat::sync::format::{FatVolumeFormatter, FormatOptions, FatTypeSelection};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::DirectoryRef;

use pyo3::prelude::*;

// 4MB chunk size is the sweet spot for maximum USB flash drive throughput
const CHUNK_SIZE: usize = 4 * 1024 * 1024; 

/// A wrapper to sandbox hadris-fat inside a specific partition.
/// This prevents it from doing absolute seeks that overwrite the GPT!
pub struct PartitionWrapper<T: Read + Write + Seek> {
    pub inner: T,
    pub offset: u64,
    pub size: u64,
}

impl<T: Read + Write + Seek> Read for PartitionWrapper<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<T: Read + Write + Seek> Write for PartitionWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Read + Write + Seek> Seek for PartitionWrapper<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let actual_pos = match pos {
            SeekFrom::Start(n) => SeekFrom::Start(self.offset + n),
            SeekFrom::Current(n) => SeekFrom::Current(n),
            // Translate End seeks to the end of the partition, not the physical disk
            SeekFrom::End(n) => {
                let partition_end = self.offset + self.size;
                SeekFrom::Start(partition_end.saturating_add_signed(n))
            }
        };
        let res = self.inner.seek(actual_pos)?;
        Ok(res.saturating_sub(self.offset))
    }
}

#[pyfunction]
pub fn write_image_dd(
    py: Python,
    image_path: String,
    device_path: String,
    callback: Py<PyAny>, 
) -> PyResult<()> {
    
    let iso_path = Path::new(&image_path);
    if !iso_path.exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
            "Image file not found: {}",
            image_path
        )));
    }

    let mut src_file = File::open(iso_path)?;
    let total_bytes = src_file.metadata()?.len();

    let dest_file_res = OpenOptions::new().write(true).open(&device_path);
    let mut dest_file = match dest_file_res {
        Ok(f) => f,
        Err(e) => {
            return Err(pyo3::exceptions::PyPermissionError::new_err(format!(
                "Failed to open device '{}'. Are you running as Administrator/Root? Error: {}",
                device_path, e
            )));
        }
    };

    let (tx, rx) = kanal::bounded::<(u64, u64)>(100);

    let handle = thread::spawn(move || -> std::io::Result<()> {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut bytes_written = 0u64;

        loop {
            let bytes_read = src_file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            dest_file.write_all(&buffer[..bytes_read])?;
            bytes_written += bytes_read as u64;

            if tx.send((bytes_written, total_bytes)).is_err() {
                break; 
            }
        }
        
        dest_file.sync_all()?;
        Ok(())
    });

    loop {
        let msg = py.detach(|| rx.recv());

        match msg {
            Ok((written, total)) => {
                let _ = callback.call1(py, (written, total));
                py.check_signals()?;
            }
            Err(_) => break,
        }
    }

    match handle.join() {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(pyo3::exceptions::PyIOError::new_err(format!(
            "I/O Error during write: {}", e
        ))),
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Writer thread panicked catastrophically",
        )),
    }
}

fn copy_recursive<T: Read + Write + Seek>(
    fs_handle: &FatFs<T>,
    iso: &IsoImage<File>,
    iso_dir_ref: DirectoryRef,
    usb_dir: &mut FatDir<T>,
    tx: &kanal::Sender<(u64, u64)>,
    bytes_written: &mut u64,
    total_bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = iso.open_dir(iso_dir_ref);

    for entry_res in dir.entries() {
        let entry = entry_res?;
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" { continue; }

        let name = entry.display_name().into_owned();
        
        if entry.is_directory() {
            let mut sub_usb_dir = fs_handle.create_dir(usb_dir, &name)?;
            let sub_iso_ref = entry.as_dir_ref(iso)?;
            copy_recursive(fs_handle, iso, sub_iso_ref, &mut sub_usb_dir, tx, bytes_written, total_bytes)?;
        } else {
            let file_entry = fs_handle.create_file(usb_dir, &name)?;
            let mut writer = fs_handle.write_file(&file_entry)?;
            let file_data = iso.read_file(&entry)?; 
            
            for chunk in file_data.chunks(CHUNK_SIZE) {
                let mut pos = 0;
                while pos < chunk.len() {
                    let n = writer.write(&chunk[pos..])?;
                    if n == 0 {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "failed to write data to USB"
                        )));
                    }
                    pos += n;
                    *bytes_written += n as u64;
                    let _ = tx.send((*bytes_written, total_bytes));
                }
            }

            writer.finish()?;
        }
    }
    Ok(())
}

#[pyfunction]
pub fn write_image_iso(
    py: Python,
    image_path: String,
    device_path: String,
    has_large_file: bool,
    callback: Py<PyAny>,
) -> PyResult<()> {
    
    // 1. Verify the ISO exists
    let iso_path = Path::new(&image_path);
    if !iso_path.exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err("ISO not found"));
    }

    // 2. Open the destination device
    let mut dest_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&device_path)
        .map_err(|e| pyo3::exceptions::PyPermissionError::new_err(format!(
            "Failed to open device '{}'. Error: {}", device_path, e
        )))?;

    let dest_size = dest_file.metadata()?.len();
    if dest_size == 0 {
        return Err(pyo3::exceptions::PyIOError::new_err("Destination device has 0 bytes."));
    }

    // ---------------------------------------------------------
    // PHASE 1: PARTITIONING (GPT)
    // ---------------------------------------------------------
    let total_sectors = dest_size / 512;
    let mut gpt = GptDisk::new(total_sectors, 512);
    let start_lba = 2048; 
    let end_lba = total_sectors - 34; 

    let partition = GptPartitionEntry::new(
        Guid::BASIC_DATA, 
        Guid::default(),
        start_lba, 
        end_lba
    );

    gpt.add_partition(partition).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!(
        "Failed to create partition: {:?}", e
    )))?;

    let protective_mbr = gpt.create_protective_mbr();
    let scheme = DiskPartitionScheme::Gpt { protective_mbr, gpt };

    DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to write GPT to device: {:?}", e))
    })?;

    dest_file.sync_all()?;

    // ---------------------------------------------------------
    // PHASE 2 & 3 WRAPPER INITIALIZATION
    // ---------------------------------------------------------
    let partition_offset_bytes = start_lba * 512;
    let partition_size_bytes = (end_lba - start_lba + 1) * 512;

    // Wrap the file so hadris-fat cannot escape the partition boundaries
    let mut wrapped_partition = PartitionWrapper {
        inner: dest_file,
        offset: partition_offset_bytes,
        size: partition_size_bytes,
    };

    // ---------------------------------------------------------
    // PHASE 2: FORMATTING
    // ---------------------------------------------------------
    if has_large_file {
        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "exFAT support for >4GB files is coming in a future update."
        ));
    } else {
        wrapped_partition.seek(SeekFrom::Start(0))?;

        let options = FormatOptions::new(partition_size_bytes)
            .with_label("LIBISO_USB")
            .with_fat_type(FatTypeSelection::Fat32);

        FatVolumeFormatter::format(&mut wrapped_partition, options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("FAT32 Format failed: {:?}", e))
        })?;
    }

    wrapped_partition.flush()?;
    println!("Phase 2 Complete: Filesystem formatted!");

    // ---------------------------------------------------------
    // PHASE 3: EXTRACTION
    // ---------------------------------------------------------
    wrapped_partition.seek(SeekFrom::Start(0))?;
    let usb_fs = FatFs::open(wrapped_partition).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open USB FS: {:?}", e))
    })?;

    let iso_path_clone = image_path.clone();
    let iso_file = File::open(&iso_path_clone)?;
    let iso_img = IsoImage::open(iso_file).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open ISO: {:?}", e))
    })?;

    let (tx, rx) = kanal::bounded::<(u64, u64)>(100);
    let total_size = Path::new(&iso_path_clone).metadata()?.len();

    thread::spawn(move || {
        let mut written = 0u64;
        let root = iso_img.root_dir();
        let mut usb_root = usb_fs.root_dir(); 
        
        if let Err(e) = copy_recursive(&usb_fs, &iso_img, root.dir_ref(), &mut usb_root, &tx, &mut written, total_size) {
            eprintln!("Extraction error: {:?}", e);
        }
    });

    loop {
        let msg = py.detach(|| rx.recv());
        match msg {
            Ok((written, total)) => {
                let _ = callback.call1(py, (written, total));
                if py.check_signals().is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}