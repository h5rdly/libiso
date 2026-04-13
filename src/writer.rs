use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom}; 
use std::path::Path;
use std::thread;

use hadris_part::{GptDisk, GptPartitionEntry, Guid, DiskPartitionScheme};
use hadris_part::scheme_io::DiskPartitionSchemeWriteExt;

use hadris_fat::sync::{FatFs, FatFsWriteExt};
use hadris_fat::sync::dir::FatDir;
// use hadris_fat::sync::Write as FatWrite;
use hadris_fat::sync::format::{FatVolumeFormatter, FormatOptions, FatTypeSelection};
// use hadris_fat::exfat::format::{ExFatFormatOptions, calculate_layout};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::DirectoryRef;

use pyo3::prelude::*;


// 4MB chunk size is the sweet spot for maximum USB flash drive throughput
const CHUNK_SIZE: usize = 4 * 1024 * 1024; 

#[pyfunction]
pub fn write_image_dd(
    py: Python,
    image_path: String,
    device_path: String,
    callback: Py<PyAny>, // <-- FIX 1: Replaced PyObject with Py<PyAny>
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

    // Open the destination device. 
    // WARNING: On Windows (\\.\PhysicalDriveX) and Linux (/dev/sdX), 
    // this will throw a PermissionError if Python is not run as Admin/root!
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

    // Create a kanal channel to stream progress updates
    let (tx, rx) = kanal::bounded::<(u64, u64)>(100);

    // Spawn the background writer thread
    let handle = thread::spawn(move || -> std::io::Result<()> {
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut bytes_written = 0u64;

        loop {
            let bytes_read = src_file.read(&mut buffer)?;
            if bytes_read == 0 {
                break; // EOF reached
            }

            dest_file.write_all(&buffer[..bytes_read])?;
            bytes_written += bytes_read as u64;

            // Send progress back to the main thread. 
            // If the main thread crashed or cancelled, this gracefully aborts.
            if tx.send((bytes_written, total_bytes)).is_err() {
                break; 
            }
        }
        
        // Force the OS to flush all hardware cache buffers to the physical USB
        // before we report 100% completion!
        dest_file.sync_all()?;
        
        Ok(())
    });

    // Main Python Thread: Listen to the channel and fire the callback
    loop {
        // FIX 2: Replaced allow_threads with detach
        // We detach the Python GIL so other Python threads can run while we wait 
        // for the background Rust thread to send the next chunk update.
        let msg = py.detach(|| rx.recv());

        match msg {
            Ok((written, total)) => {
                // We re-acquired the GIL. Fire the Python progress callback!
                callback.call1(py, (written, total))?;
                
                // Allow Python to process KeyboardInterrupt (Ctrl+C)
                py.check_signals()?;
            }
            Err(_) => {
                // The channel was closed, meaning the background thread finished.
                break;
            }
        }
    }

    // Check if the background thread succeeded or threw a low-level IO error
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
    usb_dir: &FatDir<T>,
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
            let sub_usb_dir = fs_handle.create_dir(usb_dir, &name)?;
            let sub_iso_ref = entry.as_dir_ref(iso)?;
            copy_recursive(fs_handle, iso, sub_iso_ref, &sub_usb_dir, tx, bytes_written, total_bytes)?;
        } else {
            let file_entry = fs_handle.create_file(usb_dir, &name)?;
            let mut writer = fs_handle.write_file(&file_entry)?;
            let file_data = iso.read_file(&entry)?; 
            
            for chunk in file_data.chunks(CHUNK_SIZE) {
                let mut pos = 0;
                while pos < chunk.len() {
                    // Call .write() directly. This bypasses the trait resolution 
                    // issue with .write_all() while achieving the same result.
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

    // 2. Open the destination device with exclusive write access
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
    // We assume standard 512-byte logical sectors for the USB drive.
    let total_sectors = dest_size / 512;
    
    // Create a new GPT disk layout in RAM
    let mut gpt = GptDisk::new(total_sectors, 512);
    
    // Standard 1MB alignment for flash memory (2048 sectors * 512 bytes)
    let start_lba = 2048; 
    
    // Leave the last 34 sectors for the secondary GPT backup header
    let end_lba = total_sectors - 34; 

    // Create the primary data partition (Microsoft Basic Data GUID)
    // This GUID tells Windows/Linux/macOS "Hey, this is a normal mountable flash drive!"
    let partition = GptPartitionEntry::new(
        Guid::BASIC_DATA, 
        Guid::default(), // hadris-part will zero this out, which is fine for USBs
        start_lba, 
        end_lba
    );

    // Add the partition to our layout
    gpt.add_partition(partition).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!(
        "Failed to create partition: {:?}", e
    )))?;

    // Wrap it in a protective MBR (required by the UEFI spec so older BIOSes don't think the drive is blank)
    let protective_mbr = gpt.create_protective_mbr();
    let scheme = DiskPartitionScheme::Gpt { protective_mbr, gpt };

    // BLAST THE PARTITION TABLE TO THE DRIVE!
    DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("Failed to write GPT to device: {:?}", e))
    })?;

    // Force the OS to flush the new partition table to hardware
    dest_file.sync_all()?;

   
    // ---------------------------------------------------------
    // PHASE 2: FORMATTING (FAT32)
    // ---------------------------------------------------------
    let partition_offset_bytes = start_lba * 512;
    let partition_size_bytes = (end_lba - start_lba + 1) * 512;

    if has_large_file {
        // Placeholder for exFAT logic
        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "exFAT support for >4GB files is coming in a future update."
        ));
    } else {
        // Seek to the start of our new partition
        dest_file.seek(SeekFrom::Start(partition_offset_bytes))?;

        let options = FormatOptions::new(partition_size_bytes)
            .with_label("LIBISO_USB")
            .with_fat_type(FatTypeSelection::Fat32);

        // FatVolumeFormatter::format is a sync function in hadris-fat::sync
        FatVolumeFormatter::format(&mut dest_file, options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("FAT32 Format failed: {:?}", e))
        })?;
    }

    println!("Phase 2 Complete: Filesystem formatted!");

   // ... [After Phase 2 Formatting] ...

    // ---------------------------------------------------------
    // PHASE 3: EXTRACTION
    // ---------------------------------------------------------
    dest_file.seek(SeekFrom::Start(partition_offset_bytes))?;
    
    let usb_fs = FatFs::open(dest_file).map_err(|e| {
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
        let mut usb_root = usb_fs.root_dir(); // Make this mut
        
        if let Err(e) = copy_recursive(&usb_fs, &iso_img, root.dir_ref(), &mut usb_root, &tx, &mut written, total_size) {
            eprintln!("Extraction error: {:?}", e);
        }
    });

    // Progress Loop
    loop {
        let msg = py.detach(|| rx.recv());
        match msg {
            Ok((written, total)) => {
                callback.call1(py, (written, total))?;
                py.check_signals()?;
            }
            Err(_) => break,
        }
    }

    Ok(())
}