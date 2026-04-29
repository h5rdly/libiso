use std::{
    fs::{File, OpenOptions},
    io::{Read, Write, Seek, SeekFrom, Error, ErrorKind, copy,},
    path::Path,
    thread,
    sync::{Arc, mpsc, Mutex, atomic::{AtomicBool, Ordering}},
    cell::RefCell
};

use hadris_iso::{sync::IsoImage, directory::DirectoryRef, read::DirEntry};

use pyo3::{
    prelude::*, types::PyDict, 
    exceptions::{PyRuntimeError, PyPermissionError, PyIOError, PyFileNotFoundError, PyValueError}
};

use crate::io::{
    AlignedBuffer, sys::DriveLocker, open_device, trigger_os_reread, force_unmount,
};
use crate::{fat32, verify, bootloader, udf, exfat, gpt, esd, ext4};
use crate::events::{EventMsg, ProgressStream, AbortToken};

pub const DD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const ISO_CHUNK_SIZE: usize = 100 * 1024;



pub struct PartitionWrapper<T: Read + Write + Seek> {
    pub inner: T,
    pub offset: u64,
    pub size: u64,
}

impl<T: Read + Write + Seek> Read for PartitionWrapper<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let current_pos = self.seek(SeekFrom::Current(0))?;
        if current_pos >= self.size { return Ok(0); }
        let max_read = (self.size - current_pos).min(buf.len() as u64) as usize;
        self.inner.read(&mut buf[..max_read])
    }
}

impl<T: Read + Write + Seek> Write for PartitionWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let current_pos = self.seek(SeekFrom::Current(0))?;
        if current_pos >= self.size { 
            return Err(Error::new(ErrorKind::WriteZero, "Partition boundary exceeded")); 
        }
        let max_write = (self.size - current_pos).min(buf.len() as u64) as usize;
        self.inner.write(&buf[..max_write])
    }
    fn flush(&mut self) -> std::io::Result<()> { self.inner.flush() }
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


pub fn get_clean_filename(entry: &DirEntry) -> String {
    let mut name = if entry.record.is_joliet_name() {
        entry.record.joliet_name()
    } else {
        entry.display_name().into_owned()
    };
    if let Some(pos) = name.rfind(';') { name.truncate(pos); }
    name
}


// resolve internal ISO symlinks like "../../boot/grub/grubx64.efi"
pub fn resolve_iso_path(base_dir: &str, relative: &str) -> String {

    let mut parts: Vec<&str> = base_dir.split('/').filter(|s| !s.is_empty()).collect();
    for part in relative.split('/').filter(|s| !s.is_empty()) {
        if part == "." { continue; }
        if part == ".." { parts.pop(); }
        else { parts.push(part); }
    }
    parts.join("/")
}

// -- Formatting logic

pub fn format_partition<T: Read + Write + Seek>(
    wrapped_partition: &mut PartitionWrapper<T>,
    is_exfat: bool,
    volume_label: &str,
    start_lba: u64,
) -> Result<(), String> {
    wrapped_partition.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    let part_size = wrapped_partition.size;

    if is_exfat {
        exfat::format_exfat(&mut *wrapped_partition, part_size, volume_label)
            .map_err(|e| format!("exFAT Format failed: {:?}", e))?;
    } else {
        fat32::format_fat32(&mut *wrapped_partition, part_size, volume_label, start_lba as u32)
            .map_err(|e| format!("FAT32 Format failed: {:?}", e))?;
    }
    
    wrapped_partition.flush().map_err(|e| e.to_string())?;
    wrapped_partition.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (device_path, fs_type=None, volume_label="LIBISO", partition_scheme=None))]
pub fn format_usb_drive(
    device_path: String, fs_type: Option<String>, volume_label: &str, partition_scheme: Option<String>
) -> PyResult<()> {
    let _locker = DriveLocker::new(&device_path).map_err(|e| PyPermissionError::new_err(e))?;
    let mut dest_file = OpenOptions::new().read(true).write(true).open(&device_path)?;
    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    if dest_size == 0 { return Err(PyIOError::new_err("Device has 0 bytes.")); }

    let total_sectors = dest_size / 512;
    let scheme = partition_scheme.unwrap_or_else(|| "gpt".to_string());
    if total_sectors > 0xFFFF_FFFF && scheme.eq_ignore_ascii_case("mbr") {
        return Err(PyValueError::new_err("Drive is too large (> 2TB) for standard MBR formatting."));
    }

    let is_exfat = match fs_type.as_deref() {
        Some(t) if t.eq_ignore_ascii_case("exfat") => true,
        Some(t) if t.eq_ignore_ascii_case("fat32") => false,
        _ => dest_size > 32 * 1024 * 1024 * 1024,
    };

    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&vec![0u8; 1024 * 1024])?;
    if dest_size > 1024 * 1024 {
        dest_file.seek(SeekFrom::Start(dest_size - (1024 * 1024)))?;
        dest_file.write_all(&vec![0u8; 1024 * 1024])?;
    }
    dest_file.sync_all()?;

    let geom_start_lba = 2048; 
    let geom_safe_end_lba = ((total_sectors - 34) / 2048) * 2048 - 1; 

    gpt::write_partition_table(
        &mut dest_file,
        total_sectors,
        !scheme.eq_ignore_ascii_case("mbr"),
        (geom_start_lba, geom_safe_end_lba - geom_start_lba + 1),
        None, None, is_exfat
    ).map_err(|e| PyIOError::new_err(e))?;
    dest_file.sync_all()?;

    let partition_offset = geom_start_lba * 512;
    let partition_size = (geom_safe_end_lba - geom_start_lba + 1) * 512;
    let mut wrapped_partition = PartitionWrapper { inner: dest_file, offset: partition_offset, size: partition_size };
    format_partition(&mut wrapped_partition, is_exfat, volume_label, geom_start_lba).map_err(|e| PyRuntimeError::new_err(e))?;

    Ok(())
}


// -- EFI injection aux

pub fn inject_hidden_efi<W: UsbWriter>(efi_img_bytes: Vec<u8>, target_usb_fs: &W) -> Result<(), String> {
    println!("[*] Hidden EFI image detected. Parsing raw FAT filesystem...");

    let files = fat32::read_fat_image(&efi_img_bytes)?;

    for (path, data) in files {
        if path.to_uppercase().starts_with("/EFI") || path.to_uppercase().starts_with("\\EFI") {
            println!("    Injecting: {}", path);
            let (dir_path, _) = fat32::split_path(&path);
            if !dir_path.is_empty() {
                target_usb_fs.create_dir_all(dir_path)?;
            }
            if let Ok(mut writer) = target_usb_fs.open_file_writer(&path, data.len() as u64) {
                writer.write_all(&data).map_err(|e| e.to_string())?;
                writer.flush().map_err(|e| e.to_string())?;
            }
        }
    }

    println!("[+] Hidden EFI extracted to metal successfully.");
    Ok(())
}


// -- Writing

#[pyfunction]
#[pyo3(signature = (image_path, device_path, verify_written=false, abort_token=None))]
pub fn write_image_dd(
    image_path: String, device_path: String, verify_written: bool, abort_token: Option<PyRef<'_, AbortToken>>,
) -> PyResult<ProgressStream> {

    let _locker = DriveLocker::new(&device_path).map_err(|e| PyPermissionError::new_err(e))?;
    let iso_path = Path::new(&image_path);
    if !iso_path.exists() { return Err(PyFileNotFoundError::new_err("Image not found")); }
    let total_size = iso_path.metadata()?.len();

    let mut dest_file = OpenOptions::new().write(true).open(&device_path)
        .map_err(|e| PyPermissionError::new_err(format!("Open device err: {}", e)))?;
    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    dest_file.seek(SeekFrom::Start(0))?; 
    if total_size > dest_size {
        return Err(PyValueError::new_err(format!("Target device ({} bytes) is too small for this image ({} bytes).", dest_size, total_size)));
    }
    drop(dest_file);

    let (tx, rx) = mpsc::sync_channel::<EventMsg>(100);
    let abort_flag = abort_token.map(|t| t.flag.clone()).unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    thread::spawn(move || {
        let _keep_locker = _locker; // moving locker into thread
        let mut f_iso = match File::open(&image_path) {
            Ok(f) => f, Err(e) => { let _ = tx.send(EventMsg::error(&format!("Open ISO err: {}", e))); return; }
        };
        let mut f_dev = match open_device(&device_path, true) {
            Ok(f) => f, Err(e) => { let _ = tx.send(EventMsg::error(&format!("Open device err: {}", e))); return; }
        };
        let _ = tx.send(EventMsg::phase("Raw DD Write"));

        let chunk_size = DD_CHUNK_SIZE;
        let mut buf = AlignedBuffer::new(chunk_size);
        let mut written = 0u64;

        while written < total_size {
            if abort_flag.load(Ordering::Relaxed) { let _ = tx.send(EventMsg::error("Cancelled by user")); return; }
            let to_read = std::cmp::min(chunk_size as u64, total_size - written) as usize;
            if let Err(e) = f_iso.read_exact(&mut buf[..to_read]) { let _ = tx.send(EventMsg::error(&format!("Read err: {}", e))); return; }
            if let Err(e) = f_dev.write_all(&buf[..to_read]) { let _ = tx.send(EventMsg::error(&format!("Write err: {}", e))); return; }
            let _ = f_dev.sync_all();
            written += to_read as u64;
            let _ = tx.send(EventMsg::progress(written, total_size));
        }

        let _ = tx.send(EventMsg::done("Burn Complete!"));
        if verify_written {
            let _ = tx.send(EventMsg::phase("Verifying Data"));
            drop(f_dev); 

            let mut v_iso = match File::open(&image_path) {
                Ok(f) => f, Err(e) => { let _ = tx.send(EventMsg::error(&format!("Verify open ISO err: {}", e))); return; }
            };
            f_dev = match open_device(&device_path, false) { 
                Ok(f) => f, Err(e) => { let _ = tx.send(EventMsg::error(&format!("Verify open device err: {}", e))); return; }
            };

            let mut buf_iso = vec![0u8; chunk_size];
            let mut buf_dev = vec![0u8; chunk_size];
            let mut verified = 0u64;
            let _ = tx.send(EventMsg::progress(0, total_size));

            while verified < total_size {
                if abort_flag.load(Ordering::Relaxed) {
                    let _ = tx.send(EventMsg::error("Cancelled by user during verification."));
                    return;
                }
                let to_read = std::cmp::min(chunk_size as u64, total_size - verified) as usize;
                if let Err(e) = v_iso.read_exact(&mut buf_iso[..to_read]) { let _ = tx.send(EventMsg::error(&format!("Verify ISO read err: {}", e))); return; }
                if let Err(e) = f_dev.read_exact(&mut buf_dev[..to_read]) { let _ = tx.send(EventMsg::error(&format!("Verify device read err: {}", e))); return; }

                if buf_iso[..to_read] != buf_dev[..to_read] {
                    let _ = tx.send(EventMsg::error(&format!("Data corruption detected at byte offset {}! The USB drive may be failing.", verified)));
                    return;
                }
                verified += to_read as u64;
                let _ = tx.send(EventMsg::progress(verified, total_size));
            }
            let _ = tx.send(EventMsg::done("Verification Complete!"));
        } 
        drop(f_dev); 

        if let Ok(f_reread) = OpenOptions::new().read(true).write(true).open(&device_path) {
            force_unmount(&device_path);
            if let Err(e) = trigger_os_reread(&f_reread, &image_path.clone()) {
                let _ = tx.send(EventMsg::log(&format!("OS cache flush warning (Kernel EBUSY): {}", e)));
            } else {
                let _ = tx.send(EventMsg::log("OS cache flushed successfully."));
            }
        }
    });
    Ok(ProgressStream { rx: Mutex::new(rx) })
}


#[pyfunction]
#[pyo3(signature = (image_path, device_path, has_large_file, iso_label, partition_scheme=None, 
    uefi_ntfs_path=None, persistence_size_mb=None, ext4_temp_path=None, verify_written=false, 
    unattend_xml_payload=None, target_arch=None, abort_token=None, use_sprout_bootloader=false))]
pub fn write_image_iso(
    image_path: String,
    device_path: String,
    has_large_file: bool,
    iso_label: &str, 
    partition_scheme: Option<String>, 
    uefi_ntfs_path: Option<String>,
    persistence_size_mb: Option<u64>, 
    ext4_temp_path: Option<String>,
    verify_written: bool, 
    unattend_xml_payload: Option<String>,
    target_arch: Option<String>, 
    abort_token: Option<PyRef<'_, AbortToken>>,
    use_sprout_bootloader: bool, 
) -> PyResult<ProgressStream> {
    
    let arch_selection = target_arch.unwrap_or_else(|| "all".to_string());
    let scheme = partition_scheme.unwrap_or_else(|| "mbr".to_string());

    let iso_path = Path::new(&image_path);
    if !iso_path.exists() { return Err(PyFileNotFoundError::new_err("ISO not found")); }

    let total_size = iso_path.metadata()?.len();
    let _locker = DriveLocker::new(&device_path).map_err(|e| PyPermissionError::new_err(e))?;
    let mut dest_file = OpenOptions::new().read(true).write(true).open(&device_path)
        .map_err(|e| PyPermissionError::new_err(format!("Failed to open device '{}'. Error: {}", device_path, e)))?;

    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    dest_file.seek(SeekFrom::Start(0))?; 

    if dest_size == 0 { return Err(PyIOError::new_err("Destination device has 0 bytes.")); }

    if total_size > dest_size {
        return Err(PyValueError::new_err(format!("Target device ({} bytes) is too small for this image ({} bytes).", dest_size, total_size)));
    }

    let total_sectors = dest_size / 512;
    if total_sectors > 0xFFFF_FFFF && scheme.eq_ignore_ascii_case("mbr") {
        return Err(PyValueError::new_err("MBR partition scheme does not support drives larger than 2TB."));
    }

    let start_lba = 2048; 
    let safe_end_lba = ((total_sectors - 34) / 2048) * 2048 - 1;

    let persistence_sectors = persistence_size_mb.unwrap_or(0) * 2048; 
    let efi_sectors = if has_large_file { 2048 } else { 0 };

    let part1_sectors = (safe_end_lba - start_lba + 1).saturating_sub(efi_sectors + persistence_sectors);
    let part1_end_lba = start_lba + part1_sectors - 1;
    let partition_offset_bytes = start_lba * 512;
    let partition_size_bytes = part1_sectors * 512;

    let mut next_lba = part1_end_lba + 1;

    let efi_part = if has_large_file {
        let part = (next_lba, efi_sectors);
        next_lba += efi_sectors;
        Some(part)
    } else { None };

    let persistence_part = if persistence_sectors > 0 { Some((next_lba, persistence_sectors)) } else { None };

    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&vec![0u8; 1024 * 1024])?; 
    if dest_size > 1024 * 1024 {
        dest_file.seek(SeekFrom::Start(dest_size - (1024 * 1024)))?;
        dest_file.write_all(&vec![0u8; 1024 * 1024])?; 
    }
    dest_file.sync_all()?;
    dest_file.seek(SeekFrom::Start(0))?;

    gpt::write_partition_table(
        &mut dest_file,
        total_sectors,
        !scheme.eq_ignore_ascii_case("mbr"),
        (start_lba, part1_sectors),
        efi_part,
        persistence_part,
        has_large_file
    ).map_err(|e| PyIOError::new_err(e))?;

    dest_file.sync_all()?;

    let mut lba0_bytes = [0u8; 512];
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.read_exact(&mut lba0_bytes)?;
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&[0u8; 512])?;
    dest_file.sync_all()?;

    // Create Persistence Partition if requested
    if let Some((start, sectors)) = persistence_part {
        let ext4_offset = start * 512;
        let ext4_size = sectors * 512;
        
        let temp_path_str = ext4_temp_path.ok_or_else(|| PyValueError::new_err("ext4_temp_path must be provided if persistence_size_mb is set"))?;
        let temp_ext4 = Path::new(&temp_path_str);
        
        let mut temp_file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(temp_ext4)?;
        ext4::format_ext4(&mut temp_file, ext4_size, "writable")
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to format ext4: {}", e)))?;

        // Copy the freshly formatted ext4 image directly onto the USB flash drive
        temp_file.seek(SeekFrom::Start(0))?;
        dest_file.seek(SeekFrom::Start(ext4_offset))?;
        copy(&mut temp_file, &mut dest_file)?;
        dest_file.sync_all()?;
    }

    let mut dest_file_for_uefi = dest_file.try_clone()?;

    let mut wrapped_partition = PartitionWrapper {
        inner: dest_file,
        offset: partition_offset_bytes,
        size: partition_size_bytes,
    };

    let short_usb_label = iso_label.chars().take(11).collect::<String>().replace(' ', "_").to_uppercase();
    format_partition(&mut wrapped_partition, has_large_file, short_usb_label.as_str(), start_lba)
        .map_err(|e| PyRuntimeError::new_err(e))?;
    
    let iso_path_clone = image_path.clone();

    let (tx, rx) = mpsc::sync_channel::<EventMsg>(100);
    let abort_flag = abort_token.map(|t| t.flag.clone()).unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    let device_path_clone = device_path.clone();
    let old_label_owned = iso_label.to_string();
    let new_label_owned = short_usb_label.to_string();

    if has_large_file {
        let uefi_path = uefi_ntfs_path.ok_or_else(|| PyValueError::new_err("uefi_ntfs_path must be provided for large files (exFAT mode)"))?;
        let mut uefi_file = File::open(&uefi_path)?;
        if let Some((start, _)) = efi_part {
            dest_file_for_uefi.seek(SeekFrom::Start(start * 512))?;
            copy(&mut uefi_file, &mut dest_file_for_uefi)?;
            dest_file_for_uefi.sync_all()?;
        }
    }

    thread::spawn(move || {
        let _keep_locker = _locker; 
        let mut wp = wrapped_partition;
        wp.seek(SeekFrom::Start(0)).unwrap();

        if has_large_file {
            let _ = tx.send(EventMsg::phase("Extracting image (exFAT Bare-Metal)"));
            
            let bare_fs = match exfat::BareExFat::mount(wp) {
                Ok(fs) => fs,
                Err(e) => { let _ = tx.send(EventMsg::error(&format!("Failed to mount Bare exFAT: {}", e))); return; }
            };
            
            run_burn_and_verify(
                &bare_fs, &bare_fs, &iso_path_clone, &device_path_clone, &tx, total_size, 
                has_large_file, &arch_selection, &old_label_owned, &new_label_owned, unattend_xml_payload, 
                &new_label_owned, abort_flag, verify_written, lba0_bytes, use_sprout_bootloader,
            );
          } else {
            let _ = tx.send(EventMsg::phase("Extracting image (FAT32)"));
            
            let usb_fs = match fat32::BareFat32::mount(wp) {
                Ok(fs) => fs,
                Err(e) => { let _ = tx.send(EventMsg::error(&format!("Failed to mount BareFAT32: {:?}", e))); return; }
            };
            
            run_burn_and_verify(
                &usb_fs, &usb_fs, &iso_path_clone, &device_path_clone, &tx, total_size, has_large_file, 
                &arch_selection, &old_label_owned, &new_label_owned, unattend_xml_payload, &new_label_owned, 
                abort_flag, verify_written, lba0_bytes, use_sprout_bootloader
            );
        }
    });

    Ok(ProgressStream { rx: Mutex::new(rx) })
}


// -- Extractor helper functions

pub struct ImageNode {
    pub name: String,
    pub is_dir: bool,
    pub size: u64, 
    pub symlink_target: Option<String>,
}

pub trait ImageReader {
    fn list_dir(&self, path: &str) -> Result<Vec<ImageNode>, String>;
    fn stream_file(&self, path: &str, on_chunk: &mut dyn FnMut(&[u8]) -> Result<(), String>) -> Result<(), String>;
}

pub trait UsbWriter {
    type FileWriter<'w>: Write + 'w where Self: 'w;
    fn create_dir(&self, path: &str) -> Result<(), String>;
    fn open_file_writer<'w>(&'w self, path: &str, size: u64) -> Result<Self::FileWriter<'w>, String>;

    fn create_dir_all(&self, path: &str) -> Result<(), String> {
        let path = path.trim_matches('/');
        if path.is_empty() { return Ok(()); }
        
        let mut current = String::new();
        for part in path.split('/') {
            if current.is_empty() {
                current = part.to_string();
            } else {
                current = format!("{}/{}", current, part);
            }
            // Ignore - the directory might already exist
            let _ = self.create_dir(&current); 
        }
        Ok(())
    }
}


// -- ISO9660 reader
pub struct IsoReader<'a> { pub iso: &'a IsoImage<File> }
impl<'a> IsoReader<'a> {
    fn resolve_dir(&self, path: &str) -> Result<DirectoryRef, String> {
        let mut curr = self.iso.root_dir().dir_ref();
        for part in path.trim_matches('/').split('/').filter(|s| !s.is_empty()) {
            let mut found = None;
            for entry in self.iso.open_dir(curr).entries().filter_map(Result::ok) {
                if get_clean_filename(&entry).eq_ignore_ascii_case(part) && entry.is_directory() {
                    found = entry.as_dir_ref(self.iso).ok();
                    break;
                }
            }
            curr = found.ok_or_else(|| format!("Dir not found: {}", part))?;
        }
        Ok(curr)
    }

    fn resolve_entry(&self, path: &str) -> Result<DirEntry, String> {
        let parent_path = if let Some(idx) = path.rfind('/') { &path[..idx] } else { "" };
        let file_name = if let Some(idx) = path.rfind('/') { &path[idx+1..] } else { path };
        
        let parent_ref = self.resolve_dir(parent_path)?;
        for entry in self.iso.open_dir(parent_ref).entries().filter_map(Result::ok) {
            if get_clean_filename(&entry).eq_ignore_ascii_case(file_name) {
                return Ok(entry);
            }
        }
        Err(format!("Entry not found: {}", file_name))
    }
}


impl<'a> ImageReader for IsoReader<'a> {
    fn list_dir(&self, path: &str) -> Result<Vec<ImageNode>, String> {
        let dir_ref = self.resolve_dir(path)?;
        let mut nodes = Vec::new();
        for entry in self.iso.open_dir(dir_ref).entries().filter_map(Result::ok) {
            let name = get_clean_filename(&entry);
            if name == "\x00" || name == "\x01" || name == "." || name == ".." { continue; }
            
            let mut is_dir = entry.is_directory();
            let mut size = entry.total_size() as u64;
            let mut symlink_target = None;

            // RRIP - intercept the symlink target and calculate the real file's size
            if let Some(rrip) = &entry.rrip {
                if let Some(target) = &rrip.symlink_target {
                    let resolved_path = resolve_iso_path(path, target);
                    if let Ok(target_entry) = self.resolve_entry(&resolved_path) {
                        is_dir = target_entry.is_directory();
                        size = target_entry.total_size() as u64;
                        symlink_target = Some(resolved_path);
                    }
                }
            }

            nodes.push(ImageNode { name, is_dir, size, symlink_target });
        }
        Ok(nodes)
    }

    fn stream_file(&self, path: &str, on_chunk: &mut dyn FnMut(&[u8]) -> Result<(), String>) -> Result<(), String> {
        let entry = self.resolve_entry(path)?;
        let mut chunk_buf = vec![0u8; ISO_CHUNK_SIZE];
        for extent in entry.extents() {
            let mut extent_offset = 0u64;
            let extent_len = extent.length as u64;
            while extent_offset < extent_len {
                let read_size = (extent_len - extent_offset).min(ISO_CHUNK_SIZE as u64) as usize;
                let byte_offset = (extent.sector.0 as u64 * 2048) + extent_offset;
                self.iso.read_bytes_at(byte_offset, &mut chunk_buf[..read_size]).map_err(|e| e.to_string())?;
                on_chunk(&chunk_buf[..read_size])?;
                extent_offset += read_size as u64;
            }
        }
        Ok(())
    }
}


// -- UDF READER 
pub struct UdfReader<'a> { pub file: RefCell<&'a mut File>, pub ctx: &'a udf::UdfContext }
impl<'a> ImageReader for UdfReader<'a> {
    fn list_dir(&self, path: &str) -> Result<Vec<ImageNode>, String> {
        let mut f = self.file.borrow_mut();
        let icb = if path.is_empty() || path == "/" {
            self.ctx.root_icb
        } else {
            udf::find_udf_entry(&mut f, self.ctx.partition_start, &self.ctx.root_icb, path)
                .ok_or_else(|| "UDF dir not found".to_string())?.icb
        };
        let mut nodes = Vec::new();
        for e in udf::read_directory(&mut f, self.ctx.partition_start, &icb)? {
            let name = e.name.split(';').next().unwrap_or(&e.name).to_string();
            let size = if e.is_directory { 
                0 
            } else {
                udf::get_file_size(&mut f, self.ctx.partition_start, &e.icb).unwrap_or(0)
            };
            
            nodes.push(ImageNode { name, is_dir: e.is_directory, size, symlink_target: None });
        }
        Ok(nodes)
    }
    fn stream_file(&self, path: &str, on_chunk: &mut dyn FnMut(&[u8]) -> Result<(), String>) -> Result<(), String> {
        let mut f = self.file.borrow_mut();
        let entry = udf::find_udf_entry(&mut f, self.ctx.partition_start, &self.ctx.root_icb, path)
            .ok_or_else(|| "UDF file not found".to_string())?;
        let mut chunk_buf = vec![0u8; ISO_CHUNK_SIZE];
        udf::stream_file_data(&mut f, self.ctx.partition_start, &entry, &mut chunk_buf, |c| on_chunk(c))
    }
}


#[allow(clippy::too_many_arguments)]
fn execute_extraction_workflow<R: ImageReader, W: UsbWriter>(
    reader: &R,
    writer: &W,
    tx: &mpsc::SyncSender<EventMsg>,
    total_size: u64,
    has_large_file: bool,
    arch_selection: &str,
    original_iso_label: &str,
    new_usb_label: &str,
    unattend_xml_payload: Option<String>,
    autorun_inf_label: &str,
    abort_flag: Arc<AtomicBool>,
    use_sprout_bootloader: bool,
) -> Result<(), String> {
    let mut written = 0u64;
    let mut found_kernel = None;
    let mut found_initrd = None;
    let mut found_args = None;

    // Extract Files
    copy_recursive(
        reader, writer, "", tx, &mut written, total_size, original_iso_label, new_usb_label, &mut found_kernel, 
        &mut found_initrd, &mut found_args, abort_flag.clone(), use_sprout_bootloader,
    )?;

    // Linux Sprout Bootloader 
    if !has_large_file && use_sprout_bootloader {
        if let Err(e) = bootloader::install_uefi_sprout(writer, arch_selection) {
            let _ = tx.send(EventMsg::error(&format!("Bootloader installation failed: {:?}", e)));
            return Err(e.to_string());
        }

        if let Err(e) = bootloader::write_sprout_toml(
            writer, 
            found_kernel.as_deref(), 
            found_initrd.as_deref(), 
            found_args.as_deref(),
            autorun_inf_label,
        ) {
            let _ = tx.send(EventMsg::error(&format!("Sprout config failed: {:?}", e)));
            return Err(e.to_string());
        }
    }

    // Unattend XML Injection
    if let Some(xml_contents) = unattend_xml_payload {
        let bytes = xml_contents.as_bytes();
        if let Ok(mut xml_writer) = writer.open_file_writer("autounattend.xml", bytes.len() as u64) {
            let _ = xml_writer.write_all(bytes);
            let _ = xml_writer.flush();
        }
    }

    // Autorun.inf
    if let Ok(mut autorun_writer) = writer.open_file_writer("autorun.inf", autorun_inf_label.len() as u64 + 17) {
        let autorun_content = format!("[autorun]\nlabel={}\n", autorun_inf_label);
        let _ = autorun_writer.write_all(autorun_content.as_bytes());
        let _ = autorun_writer.flush();
    }

    let _ = tx.send(EventMsg::progress(total_size, total_size));
    Ok(())
}


fn execute_verify_workflow<R: ImageReader, U: verify::UsbReader>(
    reader: &R,
    usb_reader: &U,
    tx: &mpsc::SyncSender<EventMsg>,
    total_size: u64,
    skip_bootloader: bool,
) -> Result<(), String> {

    let _ = tx.send(EventMsg::phase("Verifying Data"));
    let _ = tx.send(EventMsg::progress(0, total_size));
    let mut verified = 0u64;
    verify::verify(reader, usb_reader, "", tx, &mut verified, total_size, skip_bootloader)?;
    let _ = tx.send(EventMsg::progress(total_size, total_size));
    Ok(())
}


#[allow(clippy::too_many_arguments)]
fn run_burn_and_verify<W: UsbWriter, U: verify::UsbReader>(
    writer: &W,
    usb_reader: &U,
    iso_path: &str,
    device_path: &str,
    tx: &mpsc::SyncSender<EventMsg>,
    total_size: u64,
    has_large_file: bool,
    arch_selection: &str,
    original_iso_label: &str,
    new_usb_label: &str,
    unattend_xml_payload: Option<String>,
    autorun_inf_label: &str,
    abort_flag: Arc<AtomicBool>,
    verify_written: bool,
    lba0_bytes: [u8; 512], 
    use_sprout_bootloader: bool,
) {
    let mut file = File::open(iso_path).unwrap();
    let is_udf_valid = if let Ok(udf_ctx) = udf::mount_udf(&mut file) {
        udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
    } else { false };

    let extract_res = if is_udf_valid {
        let _ = tx.send(EventMsg::log("Using UDF Parser"));
        let udf_ctx = udf::mount_udf(&mut file).unwrap();
        let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
        execute_extraction_workflow(
            &reader, writer, tx, total_size, has_large_file, arch_selection, original_iso_label, 
            new_usb_label, unattend_xml_payload, autorun_inf_label, abort_flag, use_sprout_bootloader,
        )
    } else {
        let _ = tx.send(EventMsg::log("Using ISO9660 Parser"));
        let iso_file = File::open(iso_path).unwrap();
        let iso = IsoImage::open(iso_file).unwrap();
        let reader = IsoReader { iso: &iso };
        execute_extraction_workflow(
            &reader, writer, tx, total_size, has_large_file, arch_selection, original_iso_label, 
            new_usb_label, unattend_xml_payload, autorun_inf_label, abort_flag, use_sprout_bootloader,
        )
    };

     // Flush the OS cache
    if let Ok(mut f_reread) = OpenOptions::new().read(true).write(true).open(device_path) {
        force_unmount(device_path);
        f_reread.seek(SeekFrom::Start(0)).unwrap();
        f_reread.write_all(&lba0_bytes).unwrap();
        f_reread.sync_all().unwrap();

        if let Err(e) = trigger_os_reread(&f_reread, device_path) {
            let _ = tx.send(EventMsg::log(&format!("OS cache flush warning (Kernel EBUSY): {}", e)));
        } else {
            let _ = tx.send(EventMsg::log("OS cache flushed successfully."));
        }
    }

    if let Err(e) = extract_res {
        let _ = tx.send(EventMsg::error(&format!("Extraction error: {}", e)));
        return;
    }

    if verify_written {
        let mut file = File::open(iso_path).unwrap();
        let verify_res = if is_udf_valid {
            let udf_ctx = udf::mount_udf(&mut file).unwrap();
            let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
            execute_verify_workflow(&reader, usb_reader, tx, total_size, use_sprout_bootloader)
        } else {
            let iso_file = File::open(iso_path).unwrap();
            let iso = IsoImage::open(iso_file).unwrap();
            let reader = IsoReader { iso: &iso };
            execute_verify_workflow(&reader, usb_reader, tx, total_size, use_sprout_bootloader)
        };
        if let Err(e) = verify_res {
            let _ = tx.send(EventMsg::error(&format!("Verification error: {}", e)));
            return;
        }
    }

    let msg = if verify_written { "ISO Burn and Verify Complete!" } else { "ISO Burn Complete!" };
    let _ = tx.send(EventMsg::done(msg));
}



#[allow(clippy::too_many_arguments)]
pub fn copy_recursive<R: ImageReader, W: UsbWriter>(
    reader: &R,
    writer: &W,
    current_path: &str,
    tx: &mpsc::SyncSender<EventMsg>,
    bytes_written: &mut u64,
    total_size: u64,
    original_iso_label: &str,
    new_usb_label: &str,
    found_kernel: &mut Option<String>,  
    found_initrd: &mut Option<String>,  
    found_args: &mut Option<String>, 
    abort_flag: Arc<AtomicBool>,
    use_sprout_bootloader: bool,
) -> Result<(), String> {
    
    let entries = reader.list_dir(current_path)?;
    
    for entry in entries {
        if abort_flag.load(Ordering::Relaxed) { return Err("Cancelled by user".into()); }

        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }
        
        let new_path = if current_path.is_empty() { format!("/{}", clean_name) } else { format!("{}/{}", current_path, clean_name) };
        
        if use_sprout_bootloader && current_path.is_empty() && clean_name.eq_ignore_ascii_case("EFI")  {
            let _ = tx.send(EventMsg::log(&format!("Skipping original boot directory: {}", new_path)));
            // Add the skipped size to the progress bar so the math doesn't break
            *bytes_written += entry.size;
            let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
            continue; 
        }

        if entry.is_dir {
            writer.create_dir(&new_path)?;
            copy_recursive(reader, writer, &new_path, tx, bytes_written, total_size, original_iso_label, 
                new_usb_label, found_kernel, found_initrd, found_args, abort_flag.clone(), use_sprout_bootloader)?;
        } else {
            
            let stream_path = if let Some(target) = &entry.symlink_target {
                let _ = tx.send(EventMsg::log(&format!("Resolving symlink: {} -> {}", new_path, target)));
                target
            } else {
                &new_path
            };

            let _ = tx.send(EventMsg::log(&format!("Extracting: {}", new_path)));
            
            bootloader::detect_linux_payloads(&clean_name, current_path, found_kernel, found_initrd);
            
            // Look for configuration files that might contain the hardcoded 32-character ISO label
            let is_config = clean_name.to_lowercase().ends_with(".cfg") || clean_name.to_lowercase().ends_with(".conf");
            let is_hidden_efi = ["efi.img", "efiboot.img", "macefi.img"].iter()
                .any(|&name| clean_name.eq_ignore_ascii_case(name));

            if is_config {
                // Buffer the config file entirely in RAM before writing so we can patch the string safely!
                let mut config_data = Vec::new();
                reader.stream_file(stream_path, &mut |chunk| {
                    config_data.extend_from_slice(chunk);
                    Ok(())
                })?;
                
                let final_data = if let Ok(cfg_str) = std::str::from_utf8(&config_data) {
                    let patched = bootloader::patch_boot_labels(cfg_str, new_usb_label);
                    bootloader::scrape_boot_args(&patched, found_args, new_usb_label);
                    if patched != cfg_str {
                        let _ = tx.send(EventMsg::log(&format!("Patched boot label inside: {}", new_path)));
                    }
                    patched.into_bytes()
                } else {
                    config_data // Not valid UTF-8, dump raw
                };
                
                // Now that we have the exact byte length of the patched string, we open the writer
                let mut out_file = writer.open_file_writer(&new_path, final_data.len() as u64)?;
                out_file.write_all(&final_data).map_err(|e| e.to_string())?;
                out_file.flush().map_err(|e| e.to_string())?;
                
                // Add the *original* entry size to the progress bar to maintain UI math
                *bytes_written += entry.size;
                let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
            } else if is_hidden_efi {
                // Buffer the image into RAM while also streaming it to the USB
                let mut img_data = Vec::new();
                let mut out_file = writer.open_file_writer(&new_path, entry.size)?;
                
                reader.stream_file(stream_path, &mut |chunk| {
                    img_data.extend_from_slice(chunk);
                    out_file.write_all(chunk).map_err(|e| e.to_string())?;
                    
                    *bytes_written += chunk.len() as u64;
                    let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
                    Ok(())
                })?;
                out_file.flush().map_err(|e| e.to_string())?;
                
                // If we aren't using Sprout, inject the hidden EFI directly to the metal
                if !use_sprout_bootloader {
                    let _ = tx.send(EventMsg::log(&format!("Intercepted hidden EFI image: {}", clean_name)));
                    if let Err(e) = inject_hidden_efi(img_data, writer) {
                        let _ = tx.send(EventMsg::log(&format!("[-] Failed to inject hidden EFI: {}", e)));
                    } else {
                        let _ = tx.send(EventMsg::log("[+] Hidden EFI extracted to metal successfully."));
                    }
                }
            } else {
                // Not a config file: stream directly from disk to disk (Ultra Fast!)
                let mut out_file = writer.open_file_writer(&new_path, entry.size)?;
                reader.stream_file(stream_path, &mut |chunk| {
                    out_file.write_all(chunk).map_err(|e| e.to_string())?;
                    *bytes_written += chunk.len() as u64;
                    let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
                    Ok(())
                })?;
                out_file.flush().map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}



// -- Helper inspection logic and utils for Python


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

    wrapped_partition.seek(SeekFrom::Start(3))?;
    let mut sig = [0u8; 8];
    wrapped_partition.read_exact(&mut sig)?;
    let is_exfat = &sig == b"EXFAT   ";
    
    wrapped_partition.seek(SeekFrom::Start(0))?;

    let found_files = if is_exfat {
        let bare_fs = exfat::BareExFat::mount(wrapped_partition).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to mount bare exFAT: {}", e))
        })?;
        bare_fs.inspect_all().map_err(|e| {
            PyRuntimeError::new_err(format!("Inspection failed: {}", e))
        })?
    } else {
        let bare_fs = fat32::BareFat32::mount(wrapped_partition).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to mount bare FAT32: {}", e))
        })?;
        bare_fs.inspect_all().map_err(|e| {
            PyRuntimeError::new_err(format!("Inspection failed: {}", e))
        })?
    };

    Ok(found_files)
}


fn extract_to_fs<R: ImageReader>(reader: &R, current_path: &str, host_dir: &Path) -> Result<(), String> {
    let entries = reader.list_dir(current_path)?;
    for entry in entries {
        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }

        let new_img_path = if current_path.is_empty() { format!("/{}", clean_name) } else { format!("{}/{}", current_path, clean_name) };
        let new_host_path = host_dir.join(&clean_name);

        if entry.is_dir {
            std::fs::create_dir_all(&new_host_path).map_err(|e| e.to_string())?;
            extract_to_fs(reader, &new_img_path, &new_host_path)?;
        } else {
            let stream_path = entry.symlink_target.unwrap_or(new_img_path);
            let mut out_file = File::create(&new_host_path).map_err(|e| e.to_string())?;
            reader.stream_file(&stream_path, &mut |chunk| {
                out_file.write_all(chunk).map_err(|e| e.to_string())
            })?;
        }
    }
    Ok(())
}


#[pyfunction]
#[pyo3(signature = (image_path, extract_dir))]
pub fn extract_image(image_path: String, extract_dir: String) -> PyResult<()> {
    let host_root = Path::new(&extract_dir);
    if !host_root.exists() {
        std::fs::create_dir_all(host_root)?;
    }

    let mut file = File::open(&image_path).map_err(|e| {
        PyIOError::new_err(format!("Failed to open ISO: {}", e))
    })?;

    let is_udf_valid = if let Ok(udf_ctx) = udf::mount_udf(&mut file) {
        udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
    } else { 
        false 
    };

    if is_udf_valid {
        let udf_ctx = udf::mount_udf(&mut file).unwrap();
        let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
        extract_to_fs(&reader, "", host_root).map_err(|e| {
            PyRuntimeError::new_err(e)
        })?;
    } else {
        let iso_file = File::open(&image_path)?;
        let iso = IsoImage::open(iso_file).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to parse ISO9660: {:?}", e))
        })?;
        let reader = IsoReader { iso: &iso };
        extract_to_fs(&reader, "", host_root).map_err(|e| {
            PyRuntimeError::new_err(e)
        })?;
    }

    Ok(())
}


#[pyfunction]
#[pyo3(signature = (image_path, wim_path="sources/install.wim"))]
pub fn get_wim_info_from_iso<'py>(py: Python<'py>, image_path: String, wim_path: &str) -> PyResult<Bound<'py, PyDict>> {
    
    let wim_path_owned = wim_path.to_string();

    let wim_info_result: Option<esd::WimInfo> = py.detach(move || {
        
        let mut file = File::open(&image_path).ok()?;
        
        let is_udf_valid = if let Ok(udf_ctx) = udf::mount_udf(&mut file) {
            udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
        } else { false };

        let mut result = None;

        let mut execute_wim_scan = |reader: &dyn ImageReader| -> Result<(), String> {
            let mut current_offset = 0u64;
            let mut xml_offset = 0u64;
            let mut xml_size = 0u64;
            let mut xml_buffer = Vec::new();
            let mut header_buffer = Vec::new();

            let stream_res = reader.stream_file(&wim_path_owned, &mut |chunk| {
                if xml_size == 0 {
                    header_buffer.extend_from_slice(chunk);
                    if header_buffer.len() >= 204 {
                        let header = &header_buffer[0..204];
                        if &header[0..8] != b"MSWIM\x00\x00\x00" && &header[0..8] != b"WLPWM\x00\x00\x00" {
                            return Err("Invalid WIM Header".to_string());
                        }
                        
                        let mut size_arr = [0u8; 8];
                        size_arr[..7].copy_from_slice(&header[72..79]);
                        xml_size = u64::from_le_bytes(size_arr);
                        
                        let mut offset_arr = [0u8; 8];
                        offset_arr.copy_from_slice(&header[80..88]);
                        xml_offset = u64::from_le_bytes(offset_arr);
                        
                        if xml_size == 0 || xml_size > 50 * 1024 * 1024 {
                            return Err("Invalid XML bounds".to_string());
                        }
                    }
                }
                
                let chunk_end = current_offset + chunk.len() as u64;
                if xml_size > 0 && chunk_end > xml_offset {
                    let overlap_start = if current_offset < xml_offset { 
                        (xml_offset - current_offset) as usize 
                    } else { 0 };
                    
                    let overlap_data = &chunk[overlap_start..];
                    xml_buffer.extend_from_slice(overlap_data);
                    
                    if xml_buffer.len() as u64 >= xml_size {
                        xml_buffer.truncate(xml_size as usize);
                        result = esd::parse_xml_payload(&xml_buffer);
                        return Err("ABORT_SUCCESS".to_string());
                    }
                }

                current_offset += chunk.len() as u64;
                Ok(())
            });

            if let Err(e) = stream_res {
                if e != "ABORT_SUCCESS" { return Err(e); }
            }
            Ok(())
        };

        if is_udf_valid {
            let udf_ctx = udf::mount_udf(&mut file).unwrap();
            let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
            let _ = execute_wim_scan(&reader);
        } else {
            let iso_file = File::open(&image_path).ok()?;
            let iso = IsoImage::open(iso_file).ok()?;
            let reader = IsoReader { iso: &iso };
            let _ = execute_wim_scan(&reader);
        }

        result
    });

    let info = wim_info_result.ok_or_else(|| PyValueError::new_err("Could not parse WIM info from ISO"))?;

    let dict = PyDict::new(py);
    if let Some(arch) = info.architecture {
        dict.set_item("architecture", arch)?;
    }
    dict.set_item("editions", info.editions)?;
    dict.set_item("total_size_bytes", info.total_size_bytes)?;
    dict.set_item("raw_xml", info.raw_xml)?;
    dict.set_item("suggested_label", info.suggested_label)?;

    Ok(dict.into_any().cast_into::<PyDict>().unwrap())
}
