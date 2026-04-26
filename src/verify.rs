use std::io::{Read, Seek, SeekFrom, Write};
use std::thread;
use std::sync::{mpsc, Mutex};

use fatfs::{FileSystem, ReadWriteSeek, TimeProvider, OemCpConverter}; 
use hadris_fat::exfat::{ExFatFs, ExFatFileReader};

use pyo3::prelude::*;

use crate::writer::{ImageReader, EventMsg, ProgressStream, ISO_CHUNK_SIZE, DD_CHUNK_SIZE};
use crate::io::{AlignedBuffer};


pub trait UsbReader {
    type FileReader<'a>: Read + 'a where Self: 'a;
    fn open_file_reader<'a>(&'a self, path: &str) -> Result<Self::FileReader<'a>, String>;
}

pub struct Fat32UsbReader<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> { pub fs: &'a FileSystem<T, TP, OCC> }
pub struct Fat32FileReader<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> { inner: fatfs::File<'a, T, TP, OCC> }

impl<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> Read for Fat32FileReader<'a, T, TP, OCC> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        fatfs::Read::read(&mut self.inner, buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))
    }
}

impl<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> UsbReader for Fat32UsbReader<'a, T, TP, OCC> {
    type FileReader<'r> = Fat32FileReader<'r, T, TP, OCC> where Self: 'r;
    fn open_file_reader<'r>(&'r self, path: &str) -> Result<Self::FileReader<'r>, String> {
        let mut curr = self.fs.root_dir();
        let mut parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let target = parts.pop().unwrap_or("");
        for p in parts { 
            if !p.is_empty() { 
                curr = curr.open_dir(p).map_err(|e| format!("{:?}", e))?; 
            } 
        }
        let f = curr.open_file(target).map_err(|e| format!("{:?}", e))?;
        Ok(Fat32FileReader { inner: f })
    }
}

pub struct ExFatUsbReader<'a, T: Read + Write + Seek> { pub fs: &'a ExFatFs<T> }

impl<'a, T: Read + Write + Seek> UsbReader for ExFatUsbReader<'a, T> {
    type FileReader<'r> = ExFatFileReader<'r, T> where Self: 'r;
    fn open_file_reader<'r>(&'r self, path: &str) -> Result<Self::FileReader<'r>, String> {
        let mut curr = self.fs.root_dir();
        let mut parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let target = parts.pop().unwrap_or("");
        for p in parts { 
            if !p.is_empty() { 
                curr = curr.open_dir(p).map_err(|e| format!("{:?}", e))?; 
            } 
        }
        let entry = curr.find(target).map_err(|e| format!("{:?}", e))?.ok_or_else(|| "File not found".to_string())?;
        ExFatFileReader::new(self.fs, &entry).map_err(|e| format!("{:?}", e))
    }
}


pub fn verify<R: ImageReader, U: UsbReader>(
    iso_reader: &R,
    usb_reader: &U,
    current_path: &str,
    tx: &mpsc::SyncSender<EventMsg>,
    verified: &mut u64,
    total_size: u64,
    skip_bootloader: bool, // <-- Add this flag!
) -> Result<(), String> {
    let entries = iso_reader.list_dir(current_path)?;

    for entry in entries {
        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }
        
        let new_path = if current_path.is_empty() { format!("/{}", clean_name) } else { format!("{}/{}", current_path, clean_name) };

        if entry.is_dir {
            verify(iso_reader, usb_reader, &new_path, tx, verified, total_size, skip_bootloader)?;
        } else {
            let clean_lower = clean_name.to_lowercase();
            let mut skip_verify = matches!(clean_lower.as_str(), "sprout.toml" | "autounattend.xml" | "autorun.inf");
            
            // If we injected Sprout, skip verifying the EFI boot files
            if skip_bootloader && matches!(clean_lower.as_str(), "bootx64.efi" | "bootaa64.efi") {
                skip_verify = true;
            }

            if skip_verify {
                *verified += entry.size; 
                let _ = tx.send(EventMsg::progress(*verified, total_size));
                continue;
            }

            let mut usb_file = usb_reader.open_file_reader(&new_path)?;
            let mut file_read_so_far = 0u64;
            let mut usb_buf = vec![0u8; ISO_CHUNK_SIZE];

            // Stream from ISO, and pull the exact matching chunk from the USB drive
            iso_reader.stream_file(&new_path, &mut |iso_chunk| {
                let to_read = iso_chunk.len();
                usb_file.read_exact(&mut usb_buf[..to_read]).map_err(|e| format!("USB read err in '{}': {}", new_path, e))?;
                
                if iso_chunk != &usb_buf[..to_read] {
                    let mut mismatch_idx = 0;
                    for i in 0..to_read {
                        if iso_chunk[i] != usb_buf[i] { mismatch_idx = i; break; }
                    }
                    return Err(format!(
                        "Corruption in '{}'! Mismatch at byte offset {}. ISO byte: {:#04X}, USB byte: {:#04X}", 
                        new_path, file_read_so_far + mismatch_idx as u64, iso_chunk[mismatch_idx], usb_buf[mismatch_idx]
                    ));
                }
                
                file_read_so_far += to_read as u64;
                *verified += to_read as u64;
                let _ = tx.send(EventMsg::progress(*verified, total_size));
                Ok(())
            })?;
        }
    }
    Ok(())
}


// -- USB drive claimed size verification 

pub fn verify_hardware_capacity<T: Read + Write + Seek>(
    drive: &mut T, total_size: u64, tx: &mpsc::SyncSender<EventMsg>,
    mut sync_fn: impl FnMut(&mut T) -> Result<(), String>, 
) -> Result<(), String> {

    let chunk_size = std::cmp::min(DD_CHUNK_SIZE, total_size as usize);
    let mut buf = AlignedBuffer::new(chunk_size);
    let magic: u64 = 0xAA55AA55_DEADBEEF;

    let mut offset = 0u64;
    while offset < total_size {
        let to_write = std::cmp::min(chunk_size as u64, total_size - offset) as usize;
        for i in (0..to_write).step_by(8) {
            let val = (offset + i as u64) ^ magic;
            let copy_len = std::cmp::min(8, to_write - i);
            buf[i..i + copy_len].copy_from_slice(&val.to_le_bytes()[..copy_len]);
        }
        drive.write_all(&buf[..to_write]).map_err(|e| format!("Hardware write failed at offset {}: {}", offset, e))?;
        offset += to_write as u64;
        let _ = tx.send(EventMsg::progress(offset, total_size * 2));
    }

    sync_fn(drive)?;
    drive.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;

    offset = 0;
    while offset < total_size {
        let to_read = std::cmp::min(chunk_size as u64, total_size - offset) as usize;
        drive.read_exact(&mut buf[..to_read]).map_err(|e| format!("Hardware read failed at offset {}: {}", offset, e))?;
        for i in (0..to_read).step_by(8) {
            let current_pos = offset + i as u64;
            let expected_bytes = (current_pos ^ magic).to_le_bytes();
            let copy_len = std::cmp::min(8, to_read - i);
            if buf[i..i + copy_len] != expected_bytes[..copy_len] {
                return Err(format!("Fake drive detected! Hardware capacity spoofing found at byte offset {}.", current_pos));
            }
        }
        offset += to_read as u64;
        let _ = tx.send(EventMsg::progress(total_size + offset, total_size * 2));
    }
    Ok(())
}


pub fn verify_usb_size(device_path: &str, tx: &mpsc::SyncSender<EventMsg>,) -> Result<(), String> {
    
    let mut file = crate::io::open_device(device_path, true).map_err(|e| format!("Failed to open device: {}", e))?;
    let total_size = file.seek(SeekFrom::End(0)).map_err(|e| format!("Seek err: {}", e))?;
    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    verify_hardware_capacity(&mut file, total_size, tx, |f| f.sync_all().map_err(|e| e.to_string()))
}


#[pyfunction]
#[pyo3(signature = (device_path))]
pub fn destructive_verify_usb_size(device_path: String) -> PyResult<ProgressStream> {
    let (tx, rx) = mpsc::sync_channel::<EventMsg>(100);
    thread::spawn(move || {
        if let Err(e) = verify_usb_size(&device_path, &tx) { let _ = tx.send(EventMsg::error(&e)); }
    });
    Ok(ProgressStream { rx: Mutex::new(rx) })
}