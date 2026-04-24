use std::io::{Read, Seek, SeekFrom, Write};
use std::thread;

use fatfs::{Dir, FileSystem, ReadWriteSeek, TimeProvider, OemCpConverter}; 
use hadris_fat::exfat::{ExFatFs, ExFatDir, ExFatFileReader};

use hadris_iso::read::IsoImage;
use hadris_iso::directory::DirectoryRef;

use pyo3::prelude::*;

use crate::writer::{EventMsg, ProgressStream, DD_CHUNK_SIZE, get_clean_filename};
use crate::io::{AlignedBuffer};


// Compare a file from the ISO against a file on the USB drive byte-for-byte
pub fn verify_file_chunks(
    mut iso_file: impl Read,
    mut usb_file: impl Read,
    file_size: u64,
    file_name: &str,
    tx: &kanal::Sender<EventMsg>,
    verified_bytes: &mut u64,
    total_size: u64,
) -> Result<(), String> {

    let chunk_size = DD_CHUNK_SIZE;
    let mut buf_iso = AlignedBuffer::new(chunk_size);
    let mut buf_usb = AlignedBuffer::new(chunk_size);
    let mut read_so_far = 0u64;

    while read_so_far < file_size {
        let to_read = std::cmp::min(chunk_size as u64, file_size - read_so_far) as usize;
        
        iso_file.read_exact(&mut buf_iso[..to_read]).map_err(|e| format!("ISO read error in '{}': {}", file_name, e))?;
        usb_file.read_exact(&mut buf_usb[..to_read]).map_err(|e| format!("USB read error in '{}': {:?}", file_name, e))?;

        if buf_iso[..to_read] != buf_usb[..to_read] {
            // Find the exact byte where it diverged for ultimate debugging!
            let mut mismatch_idx = 0;
            for i in 0..to_read {
                if buf_iso[i] != buf_usb[i] {
                    mismatch_idx = i;
                    break;
                }
            }
            
            let abs_offset = read_so_far + mismatch_idx as u64;
            return Err(format!(
                "Corruption in '{}'! Mismatch at byte offset {}. ISO byte: {:#04X}, USB byte: {:#04X}", 
                file_name, abs_offset, buf_iso[mismatch_idx], buf_usb[mismatch_idx]
            ));
        }

        read_so_far += to_read as u64;
        *verified_bytes += to_read as u64;
        
        if tx.send(EventMsg::progress(*verified_bytes, total_size)).is_err() {
            return Err("Python disconnected".to_string());
        }
    }
    Ok(())
}


pub fn verify_recursive<T, U, TP, OCC>(
    usb_fs: &FileSystem<U, TP, OCC>,
    iso_img: &IsoImage<T>,
    iso_dir: DirectoryRef,
    usb_dir: &Dir<'_, U, TP, OCC>, 
    tx: &kanal::Sender<EventMsg>,
    verified: &mut u64,
    total_size: u64,
) -> Result<(), String>
where 
    T: std::io::Read + std::io::Seek,
    // tell Rust that U uses standard std::io::Error so fatfs::File implements std::io::Read
    U: ReadWriteSeek<Error = std::io::Error>, 
    TP: TimeProvider,    
    OCC: OemCpConverter, 
{
    let parsed_iso_dir = iso_img.open_dir(iso_dir);

    for entry_result in parsed_iso_dir.entries() {
        let entry = entry_result.map_err(|e| format!("ISO read err: {:?}", e))?;
        if entry.is_special() { continue; }
        let clean_name = get_clean_filename(&entry);

        if entry.is_directory() {
            let mut usb_subdir = usb_dir.open_dir(&clean_name).map_err(|e| format!("Missing USB dir '{}': {:?}", clean_name, e))?;
            let sub_ref = entry.as_dir_ref(iso_img).map_err(|e| format!("Dir ref err: {}", e))?;
            verify_recursive(usb_fs, iso_img, sub_ref, &mut usb_subdir, tx, verified, total_size)?;
        } else {
            if matches!(
                clean_name.to_lowercase().as_str(),
                "bootx64.efi" | "bootaa64.efi" | "sprout.toml" | "autounattend.xml"
            ) {
                // Skip verification for files we intentionally altered/injected
                *verified += entry.total_size() as u64; // Artificially advance the progress bar
                let _ = tx.send(EventMsg::progress(*verified, total_size)); // Advance the progress bar
                continue;
            }

            let iso_data = iso_img.read_file(&entry).map_err(|e| format!("Failed to read ISO file '{}': {}", clean_name, e))?;
            let usb_file = usb_dir.open_file(&clean_name).map_err(|e| format!("Missing USB file '{}': {:?}", clean_name, e))?;
            
            verify_file_chunks(&iso_data[..], usb_file, entry.total_size(), &clean_name, tx, verified, total_size)?;        }
    }
    Ok(())
}


pub fn verify_recursive_exfat<T, U>(
    usb_fs: &ExFatFs<U>,
    iso_img: &IsoImage<T>,
    iso_dir: DirectoryRef,
    usb_dir: &ExFatDir<'_, U>, 
    tx: &kanal::Sender<EventMsg>,
    verified: &mut u64,
    total_size: u64,
) -> Result<(), String> 
where 
    T: std::io::Read + std::io::Seek,
    U: std::io::Read + std::io::Write + std::io::Seek,
{
    let parsed_iso_dir = iso_img.open_dir(iso_dir);

    for entry_result in parsed_iso_dir.entries() {
        let entry = entry_result.map_err(|e| format!("ISO read err: {:?}", e))?;
        if entry.is_special() { continue; }
        let clean_name = get_clean_filename(&entry);

        if entry.is_directory() {
            let usb_subdir = usb_dir.open_dir(&clean_name).map_err(|e| format!("Missing USB dir '{}': {:?}", clean_name, e))?;
            let sub_ref = entry.as_dir_ref(iso_img).map_err(|e| format!("Dir ref err: {}", e))?;
            verify_recursive_exfat(usb_fs, iso_img, sub_ref, &usb_subdir, tx, verified, total_size)?;
        } else {
            if matches!(
                clean_name.to_lowercase().as_str(),
                "bootx64.efi" | "bootaa64.efi" | "sprout.toml" | "autounattend.xml"
            ) {
                // Skip verification for files we intentionally altered/injected
                *verified += entry.total_size() as u64; // Artificially advance the progress bar
                let _ = tx.send(EventMsg::progress(*verified, total_size)); // Advance the progress bar
                continue;
            }
            let iso_data = iso_img.read_file(&entry).map_err(|e| format!("Failed to read ISO file '{}': {}", clean_name, e))?;
            
            let usb_entry_opt = usb_dir.find(&clean_name).map_err(|e| format!("USB find err: {:?}", e))?;
            let usb_entry = usb_entry_opt.ok_or_else(|| format!("Missing USB file: {}", clean_name))?;
            
            let usb_file = ExFatFileReader::new(usb_fs, &usb_entry).map_err(|e| format!("Failed to open USB file: {:?}", e))?;
            
            verify_file_chunks(&iso_data[..], usb_file, entry.total_size(), &clean_name, tx, verified, total_size)?;
        }
    }
    Ok(())
}



/// The core algorithm, separated from OS files so we can test it in-memory.
pub fn verify_hardware_capacity<T: Read + Write + Seek>(
    drive: &mut T,
    total_size: u64,
    tx: &kanal::Sender<EventMsg>,
    mut sync_fn: impl FnMut(&mut T) -> Result<(), String>, 
) -> Result<(), String> {

    let chunk_size = std::cmp::min(DD_CHUNK_SIZE, total_size as usize);
    let mut buf = AlignedBuffer::new(chunk_size);
    
    // A magic 64-bit number to XOR against the offset
    let magic: u64 = 0xAA55AA55_DEADBEEF;

    // Pass 1 - write deterministic pattern
    let mut offset = 0u64;
    while offset < total_size {
        let to_write = std::cmp::min(chunk_size as u64, total_size - offset) as usize;
        
        for i in (0..to_write).step_by(8) {
            let current_pos = offset + i as u64;
            let val = current_pos ^ magic;
            let val_bytes = val.to_le_bytes();
            
            let copy_len = std::cmp::min(8, to_write - i);
            buf[i..i + copy_len].copy_from_slice(&val_bytes[..copy_len]);
        }

        drive.write_all(&buf[..to_write]).map_err(|e| format!("Hardware write failed at offset {}: {}", offset, e))?;
        offset += to_write as u64;
        
        if tx.send(EventMsg::progress(offset, total_size * 2)).is_err() {
            return Err("Python disconnected".to_string());
        }
    }

    sync_fn(drive)?;
    drive.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;

    // Pass 2 - read and verify deterministic pattern
    offset = 0;
    while offset < total_size {
        let to_read = std::cmp::min(chunk_size as u64, total_size - offset) as usize;
        
        drive.read_exact(&mut buf[..to_read]).map_err(|e| format!("Hardware read failed at offset {}: {}", offset, e))?;

        for i in (0..to_read).step_by(8) {
            let current_pos = offset + i as u64;
            let expected_val = current_pos ^ magic;
            let expected_bytes = expected_val.to_le_bytes();
            
            let copy_len = std::cmp::min(8, to_read - i);
            
            if buf[i..i + copy_len] != expected_bytes[..copy_len] {
                return Err(format!(
                    "Fake drive detected! Hardware capacity spoofing found at byte offset {}. \
                     The drive looped around and overwrote its own data.", 
                     current_pos
                ));
            }
        }

        offset += to_read as u64;
        
        if tx.send(EventMsg::progress(total_size + offset, total_size * 2)).is_err() {
            return Err("Python disconnected".to_string());
        }
    }

    Ok(())
}


// Used by the PyO3 binding
pub fn verify_usb_size(
    device_path: &str, tx: &kanal::Sender<EventMsg>,) -> Result<(), String> {
    
    let mut file = crate::io::open_device(device_path, true)
        .map_err(|e| format!("Failed to open device for hardware verification: {}", e))?;

    let total_size = file.seek(SeekFrom::End(0)).map_err(|e| format!("Seek err: {}", e))?;
    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;

    verify_hardware_capacity(&mut file, total_size, tx, |f| {
        // bypassing OS cache for real physical validation
        f.sync_all().map_err(|e| format!("Hardware sync failed: {}", e))
    })
}


#[pyfunction]
#[pyo3(signature = (device_path))]
pub fn destructive_verify_usb_size(device_path: String) -> PyResult<ProgressStream> {
    let (tx, rx) = kanal::bounded::<EventMsg>(100);

    thread::spawn(move || {
        if let Err(e) = verify_usb_size(&device_path, &tx) {
            let _ = tx.send(EventMsg::error(&e));
        }
    });

    Ok(ProgressStream { rx })
}