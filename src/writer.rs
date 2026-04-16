use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom}; 
use std::path::Path;
use std::thread;

use hadris_part::{GptDisk, GptPartitionEntry, Guid, DiskPartitionScheme};
use hadris_part::scheme_io::DiskPartitionSchemeWriteExt;

use hadris_fat::sync::{FatFs, FatFsWriteExt};
use hadris_fat::sync::dir::FatDir;
use hadris_fat::sync::format::{FatVolumeFormatter, FormatOptions, FatTypeSelection};
use hadris_fat::exfat::{ExFatFs, ExFatDir};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::DirectoryRef;

use arcbox_ext4::Formatter as Ext4Formatter;

use pyo3::prelude::*;

use crate::io::sys::DriveLocker;


const CHUNK_SIZE: usize = 4 * 1024 * 1024; 


#[pyclass]
pub struct ProgressStream {
    rx: kanal::Receiver<Result<(u64, u64), String>>,
}


#[pymethods]
impl ProgressStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python) -> PyResult<Option<(u64, u64)>> {
        // py.detach allows Python to handle Ctrl+C or other threads while waiting
        match py.detach(|| self.rx.recv()) {
            Ok(Ok((written, total))) => Ok(Some((written, total))),
            Ok(Err(e)) => Err(pyo3::exceptions::PyIOError::new_err(e)), // Thread threw an error
            Err(_) => Ok(None), // Channel closed (StopIteration in Python)
        }
    }
}

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
#[pyo3(signature = (image_path, device_path, verify_written=false))]
pub fn write_image_dd(
    image_path: String, device_path: String, verify_written: bool,
) -> PyResult<ProgressStream> {

    let iso_path = Path::new(&image_path);
    if !iso_path.exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err("Image not found"));
    }
    let total_size = iso_path.metadata()?.len();

    let (tx, rx) = kanal::bounded::<Result<(u64, u64), String>>(100);

    thread::spawn(move || {

        // Write
        let mut f_iso = match File::open(&image_path) {
            Ok(f) => f,
            Err(e) => { let _ = tx.send(Err(format!("Open ISO err: {}", e))); return; }
        };
        let mut f_dev = match OpenOptions::new().write(true).open(&device_path) {
            Ok(f) => f,
            Err(e) => { let _ = tx.send(Err(format!("Open device err: {}", e))); return; }
        };

        let chunk_size = 4 * 1024 * 1024; // 4MB chunks
        let mut buf = vec![0u8; chunk_size];
        let mut written = 0u64;

        while written < total_size {
            let to_read = std::cmp::min(chunk_size as u64, total_size - written) as usize;
            if let Err(e) = f_iso.read_exact(&mut buf[..to_read]) {
                let _ = tx.send(Err(format!("Read err: {}", e))); return;
            }
            if let Err(e) = f_dev.write_all(&buf[..to_read]) {
                let _ = tx.send(Err(format!("Write err: {}", e))); return;
            }
            
            // Sync periodically to ensure writes hit the metal
            let _ = f_dev.sync_all();

            written += to_read as u64;
            if tx.send(Ok((written, total_size))).is_err() {
                return; // Python disconnected
            }
        }

        // Verify
        if verify_written {
            // Drop the write handle so the OS completely flushes it
            drop(f_dev); 

            let mut v_iso = match File::open(&image_path) {
                Ok(f) => f,
                Err(e) => { let _ = tx.send(Err(format!("Verify open ISO err: {}", e))); return; }
            };
            let mut v_dev = match File::open(&device_path) {
                Ok(f) => f,
                Err(e) => { let _ = tx.send(Err(format!("Verify open device err: {}", e))); return; }
            };

            let mut buf_iso = vec![0u8; chunk_size];
            let mut buf_dev = vec![0u8; chunk_size];
            let mut verified = 0u64;

            // Signal to Python that verification has started by sending a 0
            let _ = tx.send(Ok((0, total_size)));

            while verified < total_size {
                let to_read = std::cmp::min(chunk_size as u64, total_size - verified) as usize;

                if let Err(e) = v_iso.read_exact(&mut buf_iso[..to_read]) {
                    let _ = tx.send(Err(format!("Verify ISO read err: {}", e))); return;
                }
                if let Err(e) = v_dev.read_exact(&mut buf_dev[..to_read]) {
                    let _ = tx.send(Err(format!("Verify device read err: {}", e))); return;
                }

                if buf_iso[..to_read] != buf_dev[..to_read] {
                    let _ = tx.send(Err(format!("Data corruption detected at byte offset {}! The USB drive may be failing.", verified)));
                    return;
                }

                verified += to_read as u64;
                if tx.send(Ok((verified, total_size))).is_err() {
                    return; // Python disconnected
                }
            }
        }
    });

    Ok(ProgressStream { rx })
}


// FAT32 extraction
fn copy_recursive<T: Read + Write + Seek>(
    fs_handle: &FatFs<T>,
    iso: &IsoImage<File>,
    iso_dir_ref: DirectoryRef,
    usb_dir: &mut FatDir<T>,
    tx: &kanal::Sender<Result<(u64, u64), String>>,
    bytes_written: &mut u64,
    total_bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = iso.open_dir(iso_dir_ref);
    let mut chunk_buf = vec![0u8; CHUNK_SIZE];

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
            
            for extent in entry.extents() {
                let mut extent_offset = 0u64;
                let extent_len = extent.length as u64;

                while extent_offset < extent_len {
                    let read_size = (extent_len - extent_offset).min(CHUNK_SIZE as u64) as usize;
                    let byte_offset = (extent.sector.0 as u64 * 2048) + extent_offset;
                    iso.read_bytes_at(byte_offset, &mut chunk_buf[..read_size])?;

                    let mut pos = 0;
                    while pos < read_size {
                        let n = writer.write(&chunk_buf[pos..read_size])?;
                        if n == 0 { return Err(Box::new(std::io::Error::new(std::io::ErrorKind::WriteZero, "USB write failure"))); }
                        pos += n;
                        *bytes_written += n as u64;
                        let _ = tx.send(Ok((*bytes_written, total_bytes)));
                    }
                    extent_offset += read_size as u64;
                }
            }
            writer.finish()?;
        }
    }
    Ok(())
}


// EXFAT extraction 
fn copy_recursive_exfat<T: Read + Write + Seek>(
    fs_handle: &ExFatFs<T>,
    iso: &IsoImage<File>,
    iso_dir_ref: DirectoryRef,
    usb_dir: &ExFatDir<'_, T>,
    tx: &kanal::Sender<Result<(u64, u64), String>>,
    bytes_written: &mut u64,
    total_bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = iso.open_dir(iso_dir_ref);
    let mut chunk_buf = vec![0u8; CHUNK_SIZE];

    for entry_res in dir.entries() {
        let entry = entry_res?;
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" { continue; }

        let name = entry.display_name().into_owned();
        
        if entry.is_directory() {
            let sub_usb_dir = fs_handle.create_dir(usb_dir, &name)?;
            let sub_iso_ref = entry.as_dir_ref(iso)?;
            copy_recursive_exfat(fs_handle, iso, sub_iso_ref, &sub_usb_dir, tx, bytes_written, total_bytes)?;
        } else {
            let file_entry = fs_handle.create_file(usb_dir, &name)?;
            let mut writer = fs_handle.write_file(&file_entry)?;
            
            for extent in entry.extents() {
                let mut extent_offset = 0u64;
                let extent_len = extent.length as u64;

                while extent_offset < extent_len {
                    let read_size = (extent_len - extent_offset).min(CHUNK_SIZE as u64) as usize;
                    let byte_offset = (extent.sector.0 as u64 * 2048) + extent_offset;
                    iso.read_bytes_at(byte_offset, &mut chunk_buf[..read_size])?;

                    let mut pos = 0;
                    while pos < read_size {
                        let n = writer.write(&chunk_buf[pos..read_size])?;
                        if n == 0 { return Err(Box::new(std::io::Error::new(std::io::ErrorKind::WriteZero, "USB write failure"))); }
                        pos += n;
                        *bytes_written += n as u64;
                        let _ = tx.send(Ok((*bytes_written, total_bytes)));
                    }
                    extent_offset += read_size as u64;
                }
            }
            writer.finish()?;
        }
    }
    Ok(())
}


// a 512-byte Master Boot Record (MBR) supporting up to 3 partitions
fn create_legacy_mbr(
    p1: (u32, u32, u8, bool), // (start, size, type, is_active)
    p2: Option<(u32, u32, u8)>,
    p3: Option<(u32, u32, u8)>,
) -> [u8; 512] {
    let mut mbr = [0u8; 512];

    let mut write_part = |offset: usize, start: u32, size: u32, ptype: u8, active: bool| {
        mbr[offset] = if active { 0x80 } else { 0x00 };
        mbr[offset + 1..offset + 4].copy_from_slice(&[0xFE, 0xFF, 0xFF]);
        mbr[offset + 4] = ptype;
        mbr[offset + 5..offset + 8].copy_from_slice(&[0xFE, 0xFF, 0xFF]);
        mbr[offset + 8..offset + 12].copy_from_slice(&start.to_le_bytes());
        mbr[offset + 12..offset + 16].copy_from_slice(&size.to_le_bytes());
    };

    write_part(446, p1.0, p1.1, p1.2, p1.3);
    
    let mut next_offset = 462;
    if let Some((start, size, ptype)) = p2 {
        write_part(next_offset, start, size, ptype, false);
        next_offset += 16;
    }
    if let Some((start, size, ptype)) = p3 {
        write_part(next_offset, start, size, ptype, false);
    }

    mbr[510] = 0x55;
    mbr[511] = 0xAA;
    mbr
}



#[pyfunction]
#[pyo3(signature = (image_path, device_path, has_large_file, partition_scheme=None, uefi_ntfs_path=None, persistence_size_mb=None, ext4_temp_path=None, verify_written=false))]
pub fn write_image_iso(
    image_path: String,
    device_path: String,
    has_large_file: bool,
    partition_scheme: Option<String>, 
    uefi_ntfs_path: Option<String>,
    persistence_size_mb: Option<u64>, 
    ext4_temp_path: Option<String>,
    verify_written: bool, 
) -> PyResult<ProgressStream> {
    
    let scheme = partition_scheme.unwrap_or_else(|| "gpt".to_string());

    let iso_path = Path::new(&image_path);
    if !iso_path.exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err("ISO not found"));
    }

    let _locker = DriveLocker::new(&device_path).map_err(|e| {
        pyo3::exceptions::PyPermissionError::new_err(e)
    })?;

    let mut dest_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&device_path)
        .map_err(|e| pyo3::exceptions::PyPermissionError::new_err(format!("Failed to open device '{}'. Error: {}", device_path, e)))?;

    let dest_size = dest_file.metadata()?.len();
    if dest_size == 0 {
        return Err(pyo3::exceptions::PyIOError::new_err("Destination device has 0 bytes."));
    }

    let total_sectors = dest_size / 512;
    if total_sectors > 0xFFFF_FFFF && scheme.eq_ignore_ascii_case("mbr") {
        return Err(pyo3::exceptions::PyValueError::new_err("MBR partition scheme does not support drives larger than 2TB."));
    }

    // Dynamic layout
    let start_lba = 2048; 
    let end_lba = total_sectors - 34; 

    let persistence_sectors = persistence_size_mb.unwrap_or(0) * 2048; 
    let efi_sectors = if has_large_file { 2048 } else { 0 };

    let part1_sectors = (end_lba - start_lba + 1).saturating_sub(efi_sectors + persistence_sectors);
    let part1_end_lba = start_lba + part1_sectors - 1;
    let partition_offset_bytes = start_lba * 512;
    let partition_size_bytes = part1_sectors * 512;

    let mut next_lba = part1_end_lba + 1;

    let efi_part = if has_large_file {
        let part = (next_lba, efi_sectors);
        next_lba += efi_sectors;
        Some(part)
    } else { None };

    let persistence_part = if persistence_sectors > 0 {
        Some((next_lba, persistence_sectors))
    } else { None };

    // GPT / MBR table routing
    if scheme.eq_ignore_ascii_case("mbr") {
        dest_file.seek(SeekFrom::Start(512))?;
        dest_file.write_all(&[0u8; 512])?;
        dest_file.seek(SeekFrom::Start((total_sectors - 1) * 512))?;
        dest_file.write_all(&[0u8; 512])?;

        let part1_type = if has_large_file { 0x07 } else { 0x0C }; 
        let p1 = (start_lba as u32, part1_sectors as u32, part1_type, true);
        
        let p2 = efi_part.map(|(start, size)| (start as u32, size as u32, 0xEF)); 
        let p3 = persistence_part.map(|(start, size)| (start as u32, size as u32, 0x83)); 
        
        let mbr_bytes = create_legacy_mbr(p1, p2, p3);
        dest_file.seek(SeekFrom::Start(0))?;
        dest_file.write_all(&mbr_bytes)?;
        dest_file.sync_all()?;
    } else {
        let mut gpt = GptDisk::new(total_sectors, 512);
        
        let part1 = GptPartitionEntry::new(Guid::BASIC_DATA, Guid::default(), start_lba, part1_end_lba);
        gpt.add_partition(part1).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Part 1 err: {:?}", e)))?;
        
        if let Some((start, size)) = efi_part {
            let part2 = GptPartitionEntry::new(Guid::EFI_SYSTEM, Guid::default(), start, start + size - 1);
            gpt.add_partition(part2).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Part 2 err: {:?}", e)))?;
        }
        
        if let Some((start, size)) = persistence_part {
            let part3 = GptPartitionEntry::new(Guid::BASIC_DATA, Guid::default(), start, start + size - 1);
            gpt.add_partition(part3).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Part 3 err: {:?}", e)))?;
        }

        let protective_mbr = gpt.create_protective_mbr();
        let scheme = DiskPartitionScheme::Gpt { protective_mbr, gpt };

        DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to write GPT: {:?}", e))
        })?;
        dest_file.sync_all()?;
    }

    // EXT4 persistence partition
    if let Some((start, sectors)) = persistence_part {
        let ext4_offset = start * 512;
        let ext4_size = sectors * 512;
        
        let temp_path_str = ext4_temp_path.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("ext4_temp_path must be provided if persistence_size_mb is set")
        })?;
        let temp_ext4 = Path::new(&temp_path_str);
        let fmt = Ext4Formatter::new(temp_ext4, 4096, ext4_size).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to init ext4 formatter: {:?}", e))
        })?;
        
        fmt.close().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to finalize ext4 formatting: {:?}", e))
        })?;

        let mut temp_file = OpenOptions::new().read(true).write(true).open(temp_ext4)?;
        temp_file.seek(SeekFrom::Start(1144))?;
        let mut label = [0u8; 16];
        label[..8].copy_from_slice(b"writable"); 
        temp_file.write_all(&label)?;

        temp_file.seek(SeekFrom::Start(0))?;
        dest_file.seek(SeekFrom::Start(ext4_offset))?;
        std::io::copy(&mut temp_file, &mut dest_file)?;
        dest_file.sync_all()?;
    }

    // FAT32 / EXFAT main extraction
    let mut dest_file_for_uefi = dest_file.try_clone()?;

    let mut wrapped_partition = PartitionWrapper {
        inner: dest_file,
        offset: partition_offset_bytes,
        size: partition_size_bytes,
    };

    let iso_path_clone = image_path.clone();
    let iso_file = File::open(&iso_path_clone)?;
    let iso_img = IsoImage::open(iso_file).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open ISO: {:?}", e))
    })?;

    let (tx, rx) = kanal::bounded::<Result<(u64, u64), String>>(100);
    let total_size = Path::new(&iso_path_clone).metadata()?.len();

    if has_large_file {
        let uefi_path = uefi_ntfs_path.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("uefi_ntfs_path must be provided for large files (exFAT mode)")
        })?;

        use hadris_fat::exfat::{ExFatFormatOptions, format_exfat};
        let options = ExFatFormatOptions::new().with_label("LIBISO_USB");
        
        wrapped_partition.seek(SeekFrom::Start(0))?;
        let exfat_fs = format_exfat(wrapped_partition, partition_size_bytes, &options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("exFAT Format failed: {:?}", e))
        })?;

        let mut uefi_file = File::open(&uefi_path)?;
        if let Some((start, _)) = efi_part {
            dest_file_for_uefi.seek(SeekFrom::Start(start * 512))?;
            std::io::copy(&mut uefi_file, &mut dest_file_for_uefi)?;
            dest_file_for_uefi.sync_all()?;
        }

        thread::spawn(move || {
            let mut written = 0u64;
            let root = iso_img.root_dir();
            let usb_root = exfat_fs.root_dir(); 
            
            // Extraction
            if let Err(e) = copy_recursive_exfat(&exfat_fs, &iso_img, root.dir_ref(), &usb_root, &tx, &mut written, total_size) {
                let _ = tx.send(Err(format!("Extraction error: {:?}", e)));
                return;
            }

            // Verification
            if verify_written {
                let _ = tx.send(Ok((0, total_size)));
                let mut verified = 0u64;
                let iso_verify_root = iso_img.root_dir();
                let usb_verify_root = exfat_fs.root_dir();
                if let Err(e) = crate::verify::verify_recursive_exfat(&exfat_fs, &iso_img, iso_verify_root.dir_ref(), &usb_verify_root, &tx, &mut verified, total_size) {
                    let _ = tx.send(Err(format!("Verification error: {}", e)));
                }
            }
        });
    } else {
        wrapped_partition.seek(SeekFrom::Start(0))?;

        let options = FormatOptions::new(partition_size_bytes)
            .with_label("LIBISO_USB")
            .with_fat_type(FatTypeSelection::Fat32);

        FatVolumeFormatter::format(&mut wrapped_partition, options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("FAT32 Format failed: {:?}", e))
        })?;

        wrapped_partition.seek(SeekFrom::Start(510))?;
        wrapped_partition.write_all(&[0x55, 0xAA])?;
        wrapped_partition.flush()?;

        wrapped_partition.seek(SeekFrom::Start(0))?;
        let usb_fs = FatFs::open(wrapped_partition).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open USB FS: {:?}", e))
        })?;

        thread::spawn(move || {
            let mut written = 0u64;
            let root = iso_img.root_dir();
            let mut usb_root = usb_fs.root_dir(); 
            
            // Extraction
            if let Err(e) = copy_recursive(&usb_fs, &iso_img, root.dir_ref(), &mut usb_root, &tx, &mut written, total_size) {
                let _ = tx.send(Err(format!("Extraction error: {:?}", e)));
                return; // <--- IMPORTANT: Abort if extraction failed
            }

            // Verification
            if verify_written {
                let _ = tx.send(Ok((0, total_size)));
                let mut verified = 0u64;
                let iso_verify_root = iso_img.root_dir();
                let mut usb_verify_root = usb_fs.root_dir();
                if let Err(e) = crate::verify::verify_recursive(&usb_fs, &iso_img, iso_verify_root.dir_ref(), &mut usb_verify_root, &tx, &mut verified, total_size) {
                    let _ = tx.send(Err(format!("Verification error: {}", e)));
                }
            }
        });
    }

    Ok(ProgressStream { rx })
}