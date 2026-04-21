use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom}; 
use std::path::Path;
use std::thread;

use hadris_part::{GptDisk, GptPartitionEntry, Guid, DiskPartitionScheme};
use hadris_part::scheme_io::DiskPartitionSchemeWriteExt;

use fatfs::{Dir, FileSystem, FormatVolumeOptions, FsOptions, FatType, format_volume,
    StdIoWrapper, ReadWriteSeek, TimeProvider, OemCpConverter}; 

use hadris_fat::exfat::{ExFatFs, ExFatDir, ExFatFormatOptions, format_exfat};

use hadris_iso::sync::IsoImage;
use hadris_iso::directory::{DirectoryRef};
use hadris_iso::read::DirEntry;

use arcbox_ext4::Formatter as Ext4Formatter;

use pyo3::prelude::*;

use crate::io::{AlignedBuffer, sys::DriveLocker, open_device};
use crate::verify;
use crate::bootloader;


pub const DD_CHUNK_SIZE: usize = 64 * 1024 * 1024;   // Optimize for raw throughput in DD mode
pub const ISO_CHUNK_SIZE: usize = 100 * 1024;        // Optimize for fast file-by-file extraction



#[pyclass]
pub struct ProgressStream {
    pub(crate) rx: kanal::Receiver<Result<(u64, u64), String>>,
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


// -- General writer helpers 

pub fn get_clean_filename(entry: &DirEntry) -> String {
    let mut name = if entry.record.is_joliet_name() {
        entry.record.joliet_name()
    } else {
        entry.display_name().into_owned()
    };

    // Strip the ISO9660 version number (e.g., "FILE.TXT;1" -> "FILE.TXT")
    if let Some(pos) = name.rfind(';') {
        name.truncate(pos);
    }

    name
}


// -- ISO mode helper functions

// FAT32 extraction
fn copy_recursive<T, TP, OCC>(
    iso: &IsoImage<File>,
    iso_dir_ref: DirectoryRef,
    usb_dir: &Dir<'_, T, TP, OCC>, 
    tx: &kanal::Sender<Result<(u64, u64), String>>,
    bytes_written: &mut u64,
    total_size: u64,
    current_path: &str,                 
    found_kernel: &mut Option<String>,  
    found_initrd: &mut Option<String>,  
) -> Result<(), Box<dyn std::error::Error>> 
where 
    T: ReadWriteSeek<Error = std::io::Error>,
    TP: TimeProvider,
    OCC: OemCpConverter,
{
    let dir = iso.open_dir(iso_dir_ref);
    let mut chunk_buf = vec![0u8; ISO_CHUNK_SIZE];

    for entry_res in dir.entries() {
        let entry = entry_res?;
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" { continue; }
        
        let clean_name = get_clean_filename(&entry);
        
        // Build the current path for the scanner
        let new_path = if current_path.is_empty() {
            format!("/{}", clean_name)
        } else {
            format!("{}/{}", current_path, clean_name)
        };
        
        if entry.is_directory() {
            let sub_usb_dir = usb_dir.create_dir(&clean_name)?; 
            let sub_iso_ref = entry.as_dir_ref(iso)?;
            // Pass the trackers down
            copy_recursive(iso, sub_iso_ref, &sub_usb_dir, tx, bytes_written, total_size, &new_path, found_kernel, found_initrd)?;        
        } else {
            let mut writer = usb_dir.create_file(&clean_name)?;
            writer.truncate()?;
            
            // Look for the kernel as we extract!
            bootloader::detect_linux_payloads(&clean_name, current_path, found_kernel, found_initrd);
            
            for extent in entry.extents() {
                let mut extent_offset = 0u64;
                let extent_len = extent.length as u64;

                while extent_offset < extent_len {
                    let read_size = (extent_len - extent_offset).min(ISO_CHUNK_SIZE as u64) as usize;
                    let byte_offset = (extent.sector.0 as u64 * 2048) + extent_offset;
                    iso.read_bytes_at(byte_offset, &mut chunk_buf[..read_size])?;

                    let mut pos = 0;
                    while pos < read_size {
                        let n = writer.write(&chunk_buf[pos..read_size])?;
                        if n == 0 { return Err(Box::new(std::io::Error::new(std::io::ErrorKind::WriteZero, "USB write failure"))); }
                        pos += n;
                        *bytes_written += n as u64;
                        let _ = tx.send(Ok((*bytes_written, total_size)));
                    }
                    extent_offset += read_size as u64;
                }
            }
            writer.flush()?;
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
    let mut chunk_buf = vec![0u8; ISO_CHUNK_SIZE];

    for entry_res in dir.entries() {
        let entry = entry_res?;
        if entry.record.name() == b"\x00" || entry.record.name() == b"\x01" { continue; }

        let clean_name = get_clean_filename(&entry);
        
        if entry.is_directory() {
            let sub_usb_dir = fs_handle.create_dir(usb_dir, &clean_name)?;
            let sub_iso_ref = entry.as_dir_ref(iso)?;
            copy_recursive_exfat(fs_handle, iso, sub_iso_ref, &sub_usb_dir, tx, bytes_written, total_bytes)?;
        } else {
            let file_entry = fs_handle.create_file(usb_dir, &clean_name)?;
            let mut writer = fs_handle.write_file(&file_entry)?;
            
            for extent in entry.extents() {
                let mut extent_offset = 0u64;
                let extent_len = extent.length as u64;

                while extent_offset < extent_len {
                    let read_size = (extent_len - extent_offset).min(ISO_CHUNK_SIZE as u64) as usize;
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



// -- Write image DD / ISO

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

    // Size check
    let mut dest_file = OpenOptions::new().write(true).open(&device_path)
        .map_err(|e| pyo3::exceptions::PyPermissionError::new_err(format!("Open device err: {}", e)))?;
    // Seek to the end of the block device to get its true physical capacity.
    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    dest_file.seek(SeekFrom::Start(0))?; // Reset cursor for writing
    
    if total_size > dest_size {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Target device ({} bytes) is too small for this image ({} bytes).", 
            dest_size, total_size
        )));
    }
    drop(dest_file);

    let (tx, rx) = kanal::bounded::<Result<(u64, u64), String>>(100);

    thread::spawn(move || {

        // Write
        let mut f_iso = match File::open(&image_path) {
            Ok(f) => f,
            Err(e) => { let _ = tx.send(Err(format!("Open ISO err: {}", e))); return; }
        };
        let mut f_dev = match open_device(&device_path, true) {
            Ok(f) => f,
            Err(e) => { let _ = tx.send(Err(format!("Open device err: {}", e))); return; }
        };

        let chunk_size = DD_CHUNK_SIZE;
        let mut buf = AlignedBuffer::new(chunk_size);
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
            let mut v_dev = match crate::io::open_device(&device_path, false) { // false = read-only
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


#[pyfunction]
#[pyo3(signature = (image_path, device_path, has_large_file, partition_scheme=None, uefi_ntfs_path=None, persistence_size_mb=None, ext4_temp_path=None, verify_written=false, unattend_xml_payload=None, target_arch=None))]
pub fn write_image_iso(
    image_path: String,
    device_path: String,
    has_large_file: bool,
    partition_scheme: Option<String>, 
    uefi_ntfs_path: Option<String>,
    persistence_size_mb: Option<u64>, 
    ext4_temp_path: Option<String>,
    verify_written: bool, 
    unattend_xml_payload: Option<String>,
    target_arch: Option<String>, 
) -> PyResult<ProgressStream> {
    
    let arch_selection = target_arch.unwrap_or_else(|| "all".to_string());

    let scheme = partition_scheme.unwrap_or_else(|| "gpt".to_string());

    let iso_path = Path::new(&image_path);
    if !iso_path.exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err("ISO not found"));
    }

    let total_size = iso_path.metadata()?.len();
    let _locker = DriveLocker::new(&device_path).map_err(|e| {
        pyo3::exceptions::PyPermissionError::new_err(e)
    })?;

    let mut dest_file = OpenOptions::new().read(true).write(true).open(&device_path)
        .map_err(|e| pyo3::exceptions::PyPermissionError::new_err(format!("Failed to open device '{}'. Error: {}", device_path, e)))?;

    // Seek to the end of the block device to get its true physical capacity.
    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    dest_file.seek(SeekFrom::Start(0))?; // Reset cursor for writing!

    if dest_size == 0 {
        return Err(pyo3::exceptions::PyIOError::new_err("Destination device has 0 bytes."));
    }

    if total_size > dest_size {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Target device ({} bytes) is too small for this image ({} bytes).", 
            dest_size, total_size
        )));
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

    // Wipe old partition tables and filesystem signatures
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&vec![0u8; 1024 * 1024])?; // Wipe first 1MB
    
    if dest_size > 1024 * 1024 {
        dest_file.seek(SeekFrom::Start(dest_size - (1024 * 1024)))?;
        dest_file.write_all(&vec![0u8; 1024 * 1024])?; // Wipe last 1MB
    }
    dest_file.sync_all()?;

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

            if let Some(xml_contents) = unattend_xml_payload {
                match exfat_fs.create_file(&usb_root, "autounattend.xml") {
                    Ok(xml_entry) => {
                        if let Ok(mut xml_writer) = exfat_fs.write_file(&xml_entry) {
                            let _ = xml_writer.write(xml_contents.as_bytes());
                            let _ = xml_writer.finish();
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(format!("XML injection error: {:?}", e)));
                        return;
                    }
                }
            }

            let _ = tx.send(Ok((total_size, total_size))); // Snap extraction progress to 100%

            // Verification
            if verify_written {
                let _ = tx.send(Ok((0, total_size)));
                let mut verified = 0u64;
                let iso_verify_root = iso_img.root_dir();
                let usb_verify_root = exfat_fs.root_dir();
                if let Err(e) = verify::verify_recursive_exfat(&exfat_fs, &iso_img, iso_verify_root.dir_ref(), &usb_verify_root, &tx, &mut verified, total_size) {
                    let _ = tx.send(Err(format!("Verification error: {}", e)));
                }
            }
        });
    } else {
        wrapped_partition.seek(SeekFrom::Start(0))?;

        // Wrap the partition so fatfs understands the standard IO traits
        let mut fatfs_partition = StdIoWrapper::new(wrapped_partition);

        let options = FormatVolumeOptions::new()
            .volume_label(*b"LIBISO_USB ") // Note: strictly 11 bytes padded with spaces
            .fat_type(FatType::Fat32);

        format_volume(&mut fatfs_partition, options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("FAT32 Format failed: {:?}", e))
        })?;

        // Extract the wrapped partition back to write the legacy signature
        let mut wrapped_partition = fatfs_partition.into_inner();
        wrapped_partition.seek(SeekFrom::Start(510))?;
        wrapped_partition.write_all(&[0x55, 0xAA])?;
        wrapped_partition.flush()?;
        wrapped_partition.seek(SeekFrom::Start(0))?;

        // Re-wrap and mount the filesystem
        let fatfs_partition = StdIoWrapper::new(wrapped_partition);
        let usb_fs = FileSystem::new(fatfs_partition, FsOptions::new()).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to mount FAT32: {:?}", e))
        })?;

        thread::spawn(move || {
            let mut written = 0u64;
            let root = iso_img.root_dir();
            let usb_root = usb_fs.root_dir(); 
            
            // Trackers for our Bootloader to find the Linux payload
            let mut found_kernel = None;
            let mut found_initrd = None;
            
            if let Err(e) = copy_recursive(&iso_img, root.dir_ref(), &usb_root, &tx, &mut written, total_size, "", &mut found_kernel, &mut found_initrd) {
                let _ = tx.send(Err(format!("Extraction error: {:?}", e)));
                return; 
            }

            // Inject Sprout Bootloader for Linux
            if !has_large_file {
                if let Err(e) = bootloader::install_uefi_sprout(&usb_root, &arch_selection) {
                    let _ = tx.send(Err(format!("Bootloader installation failed: {:?}", e)));
                    return;
                }

                // Pass the discovered paths to the TOML generator
                if let Err(e) = bootloader::write_sprout_toml(
                    &usb_root, 
                    found_kernel.as_deref(), 
                    found_initrd.as_deref(), 
                    Some("root=live:CDLABEL=LIBISO_USB ro quiet splash") // Optional boot args
                ) {
                    let _ = tx.send(Err(format!("Sprout config failed: {:?}", e)));
                    return;
                }
            }

            // Unattend XML Injection
            if let Some(xml_contents) = unattend_xml_payload {
                if let Ok(mut xml_writer) = usb_root.create_file("autounattend.xml") {
                    let _ = xml_writer.truncate();
                    let _ = xml_writer.write_all(xml_contents.as_bytes());
                }
            }

            let _ = tx.send(Ok((total_size, total_size))); // Snap extraction progress to 100%

            // Verification
            if verify_written {
                let _ = tx.send(Ok((0, total_size)));
                let mut verified = 0u64;
                let iso_verify_root = iso_img.root_dir();
                let mut usb_verify_root = usb_fs.root_dir();
                if let Err(e) = verify::verify_recursive(&usb_fs, &iso_img, iso_verify_root.dir_ref(), &mut usb_verify_root, &tx, &mut verified, total_size) {
                    let _ = tx.send(Err(format!("Verification error: {}", e)));
                }
            }
        });
    }

    Ok(ProgressStream { rx })
}



#[pyfunction]
#[pyo3(signature = (device_path, fs_type=None, volume_label="LIBISO"))]
pub fn format_usb_drive(device_path: String, fs_type: Option<String>, volume_label: &str
) -> PyResult<()> {

    let _locker = DriveLocker::new(&device_path).map_err(|e| {
        pyo3::exceptions::PyPermissionError::new_err(e)
    })?;

    let mut dest_file = OpenOptions::new().read(true).write(true).open(&device_path)?;
    let dest_size = dest_file.seek(SeekFrom::End(0))?;

    if dest_size == 0 {
        return Err(pyo3::exceptions::PyIOError::new_err("Device has 0 bytes."));
    }

    let total_sectors = dest_size / 512;
    if total_sectors > 0xFFFF_FFFF {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Drive is too large (> 2TB) for standard MBR formatting."
        ));
    }

    // Auto-select filesystem based on size (32GB boundary)
    let is_exfat = match fs_type.as_deref() {
        Some(t) if t.eq_ignore_ascii_case("exfat") => true,
        Some(t) if t.eq_ignore_ascii_case("fat32") => false,
        _ => dest_size > 32 * 1024 * 1024 * 1024,
    };

    // Wipe old signatures and filesystem
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&vec![0u8; 1024 * 1024])?;
    if dest_size > 1024 * 1024 {
        dest_file.seek(SeekFrom::Start(dest_size - (1024 * 1024)))?;
        dest_file.write_all(&vec![0u8; 1024 * 1024])?;
    }
    dest_file.sync_all()?;

    // Create MBR 
    let start_lba = 2048;
    let part_sectors = total_sectors.saturating_sub(start_lba);
    let p_type = if is_exfat { 0x07 } else { 0x0C };
    
    // Use the function already defined in your writer.rs
    let p1 = (start_lba as u32, part_sectors as u32, p_type, true);
    let mbr_bytes = create_legacy_mbr(p1, None, None);
    
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&mbr_bytes)?;
    dest_file.sync_all()?;

    // Format the partition
    let partition_offset = start_lba * 512;
    let partition_size = part_sectors * 512;

    let mut wrapped_partition = PartitionWrapper {
        inner: dest_file,
        offset: partition_offset,
        size: partition_size,
    };

    wrapped_partition.seek(SeekFrom::Start(0))?;

    if is_exfat {
        let options = ExFatFormatOptions::new().with_label(volume_label);
        format_exfat(wrapped_partition, partition_size, &options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("exFAT Format failed: {:?}", e))
        })?;
    } else {
        // Pad the volume label to exactly 11 bytes for FAT32
        let mut label_bytes = [b' '; 11];
        let bytes = volume_label.as_bytes();
        let copy_len = std::cmp::min(bytes.len(), 11);
        label_bytes[..copy_len].copy_from_slice(&bytes[..copy_len]);

        let mut fatfs_partition = fatfs::StdIoWrapper::new(wrapped_partition);

        let options = FormatVolumeOptions::new()
            .volume_label(label_bytes)
            .fat_type(FatType::Fat32);
            
        format_volume(&mut fatfs_partition, options).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("FAT32 Format failed: {:?}", e))
        })?;
        
        // Add the boot signature to the FAT32 boot sector
        let mut wrapped_partition = fatfs_partition.into_inner();
        wrapped_partition.seek(SeekFrom::Start(510))?;
        wrapped_partition.write_all(&[0x55, 0xAA])?;
        wrapped_partition.flush()?;
    }

    Ok(())
}



#[pyfunction]
#[pyo3(signature = (device_path))]
pub fn inspect_usb_partition(device_path: String) -> PyResult<Vec<String>> {
    let mut file = OpenOptions::new().read(true).open(&device_path)?;
    
    let partition_offset = 2048 * 512;
    let dest_size = file.seek(SeekFrom::End(0))?;
    let partition_size = dest_size.saturating_sub(partition_offset);

    let mut wrapped_partition = PartitionWrapper {
        inner: file,
        offset: partition_offset,
        size: partition_size,
    };

    // Detect filesystem
    wrapped_partition.seek(SeekFrom::Start(3))?;
    let mut sig = [0u8; 8];
    wrapped_partition.read_exact(&mut sig)?;
    let is_exfat = &sig == b"EXFAT   ";
    
    // Rewind back to 0 
    wrapped_partition.seek(SeekFrom::Start(0))?;

    let mut found_files = Vec::new();

    if is_exfat {
        let fs = ExFatFs::open(wrapped_partition).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to mount exFAT: {:?}", e))
        })?;
        let root = fs.root_dir();
        
        for entry_res in root.entries() {
            if let Ok(entry) = entry_res {
                found_files.push(format!("[exFAT] {}", entry.name));
            }
        }
    } else {
        // Wrap the partition for fatfs
        let fatfs_partition = fatfs::StdIoWrapper::new(wrapped_partition);
        
        let fs = FileSystem::new(fatfs_partition, FsOptions::new()).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to mount FAT32: {:?}", e))
        })?;
        let root = fs.root_dir();
        
        for entry_res in root.iter() {
            if let Ok(entry) = entry_res {
                let is_dir = if entry.is_dir() { "DIR " } else { "FILE" };
                let short = entry.short_file_name();
                let long = entry.file_name();
                found_files.push(format!(
                    "[{}] Short: '{}' | Long: '{}'", 
                    is_dir, short.trim(), long
                ));
            }
        }
    }

    Ok(found_files)
}