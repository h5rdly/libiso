use std::{
    fs::{File, OpenOptions},
    io::{Read, Write, Seek, SeekFrom, Error, ErrorKind, copy,},
    path::Path,
    thread,
    sync::{Arc, mpsc, Mutex, atomic::{AtomicBool, Ordering}},
    cell::RefCell, collections::HashSet,
};

use hadris_iso::{sync::IsoImage,};

use pyo3::{
    prelude::*,
    exceptions::{PyRuntimeError, PyPermissionError, PyIOError, PyFileNotFoundError, PyValueError}
};

use crate::io::{
    AlignedBuffer, sys::DriveLocker, open_device, trigger_os_reread, force_unmount,
};
use crate::image_parser::{ImageReader, IsoReader, UdfReader, DD_CHUNK_SIZE};
use crate::{fat32, verify, bootloader, udf, exfat, gpt, ext4, initramfs_patcher, grub_patcher};
use crate::events::{EventMsg, ProgressStream, AbortToken};



thread_local! {
    pub static WRITTEN_PATHS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}



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

    let mut lba0_bytes = [0u8; 512];
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.read_exact(&mut lba0_bytes)?;
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&[0u8; 512])?;
    dest_file.sync_all()?;

    let partition_offset = geom_start_lba * 512;
    let partition_size = (geom_safe_end_lba - geom_start_lba + 1) * 512;
    let mut wrapped_partition = PartitionWrapper { inner: dest_file, offset: partition_offset, size: partition_size };
    format_partition(&mut wrapped_partition, is_exfat, volume_label, geom_start_lba).map_err(|e| PyRuntimeError::new_err(e))?;

    // Restore LBA 0 and tell the OS to wake up and look at the new filesystem
    let mut dest_file = wrapped_partition.inner;
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&lba0_bytes)?;
    dest_file.sync_all()?;
    
    // Attempt to trigger udev/udisks2 reread
    let _ = trigger_os_reread(&dest_file, &device_path);

    Ok(())
}


// -- Pre-write analyzing logic


#[pyclass(skip_from_py_object)]
#[derive(Clone, Debug, Default)]
pub struct IsoBootProfile {
    #[pyo3(get)]
    pub has_full_efi: bool,
    #[pyo3(get)]
    pub has_stub_efi: bool,
    #[pyo3(get)]
    pub has_hidden_efi: bool,
    #[pyo3(get)]
    pub requires_sprout: bool,
}


// -- EFI injection aux

pub fn inject_hidden_efi<W: UsbWriter>(
    efi_img_bytes: Vec<u8>, 
    target_usb_fs: &W,
    tx: &mpsc::SyncSender<EventMsg>,
    new_usb_label: &str 
) -> Result<(), String> {

    println!("[*] Hidden EFI image detected. Parsing raw FAT filesystem...");

    let files = fat32::read_fat_image(&efi_img_bytes)?;

    for (path, mut data) in files {

        let path_upper = path.to_uppercase().replace('\\', "/").replace("//", "/");
        
        if path_upper.starts_with("/EFI") {
            let is_new_file = WRITTEN_PATHS.with(|paths| paths.borrow_mut().insert(path_upper.clone()));
            if !is_new_file {
                let _ = tx.send(EventMsg::log(&format!("    [CACHE HIT] Blocked duplicate: {}", path_upper)));
                continue; 
            }
            
            let _ = tx.send(EventMsg::log(&format!("    [CACHE MISS] Extracting: {}", path_upper)));
            
            let (dir_path, _) = fat32::split_path(&path);
            if !dir_path.is_empty() {
                target_usb_fs.create_dir_all(dir_path)?;
            }

            // If the extracted file is an EFI executable, patch it before writing it to the USB!
            if path_upper.ends_with(".EFI") {
                if grub_patcher::patch_memdisk_uuid_to_label(&mut data, new_usb_label) {
                    let _ = tx.send(EventMsg::log(&format!("    [+] Binary-patched memdisk UUID search in {}", path)));
                }
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
    unattend_xml_payload=None, target_arch=None, abort_token=None, use_sprout_bootloader=false,
    kernel_version_for_patch=None, fat_date=0, fat_time=0))]
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
    kernel_version_for_patch: Option<String>,
    fat_date: u16, 
    fat_time: u16,
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

    // dest_file.sync_all()?;

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
            
            burn_and_verify(
                &bare_fs, &bare_fs, &iso_path_clone, &device_path_clone, &tx, total_size, 
                has_large_file, &arch_selection, &old_label_owned, &new_label_owned, unattend_xml_payload, 
                &new_label_owned, abort_flag, verify_written, lba0_bytes, use_sprout_bootloader, 
                kernel_version_for_patch,
            );
          } else {
            let _ = tx.send(EventMsg::phase("Extracting image (FAT32)"));
            
            let usb_fs = match fat32::BareFat32::mount(wp, fat_date, fat_time) {
                Ok(fs) => fs,
                Err(e) => { let _ = tx.send(EventMsg::error(&format!("Failed to mount BareFAT32: {:?}", e))); return; }
            };
            
            burn_and_verify(
                &usb_fs, &usb_fs, &iso_path_clone, &device_path_clone, &tx, total_size, has_large_file, 
                &arch_selection, &old_label_owned, &new_label_owned, unattend_xml_payload, &new_label_owned, 
                abort_flag, verify_written, lba0_bytes, use_sprout_bootloader, 
                kernel_version_for_patch,
            );
        }
    });

    Ok(ProgressStream { rx: Mutex::new(rx) })
}


// -- Extractor helper functions


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
    mut use_sprout_bootloader: bool,
    iso_path: &str,
    kernel_version_for_patch: Option<String>,
) -> Result<(), String> {

    WRITTEN_PATHS.with(|paths| paths.borrow_mut().clear());

    let mut written = 0u64;
    let mut found_kernel = None;
    let mut found_initrd = None;
    let mut found_args = None;

    // Extract Files
    copy_recursive(
        reader, writer, "", tx, &mut written, total_size, original_iso_label, new_usb_label, &mut found_kernel, 
        &mut found_initrd, &mut found_args, abort_flag.clone(), &mut use_sprout_bootloader, 
        iso_path, &kernel_version_for_patch,
    )?;

    // Was a real GRUB bootloader written
    let efi_written = WRITTEN_PATHS.with(|paths| {
        paths.borrow().contains("/EFI/BOOT/BOOTX64.EFI") || paths.borrow().contains("/EFI/BOOT/BOOTAA64.EFI")
    });
    if !efi_written && !use_sprout_bootloader {
        let _ = tx.send(EventMsg::log("[!] No valid native EFI bootloader found (Stub-only ISO). Auto-enabling Sprout fallback."));
        use_sprout_bootloader = true;
    }

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
            kernel_version_for_patch.is_some(),
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
fn burn_and_verify<W: UsbWriter, U: verify::UsbReader>(
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
    kernel_version_for_patch: Option<String>,
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
            iso_path, kernel_version_for_patch,
        )
    } else {
        let _ = tx.send(EventMsg::log("Using ISO9660 Parser"));
        let iso_file = File::open(iso_path).unwrap();
        let iso = IsoImage::open(iso_file).unwrap();
        let reader = IsoReader { iso: &iso };
        execute_extraction_workflow(
            &reader, writer, tx, total_size, has_large_file, arch_selection, original_iso_label, 
            new_usb_label, unattend_xml_payload, autorun_inf_label, abort_flag, use_sprout_bootloader,
            iso_path, kernel_version_for_patch,
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
    use_sprout_bootloader: &mut bool,
    iso_path: &str,
    kernel_version_for_patch: &Option<String>,
) -> Result<(), String> {
    
    let entries = reader.list_dir(current_path)?;
    
    for entry in entries {
        if abort_flag.load(Ordering::Relaxed) { return Err("Cancelled by user".into()); }

        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }
        
        let new_path = if current_path.is_empty() { format!("/{}", clean_name) } else { format!("{}/{}", current_path, clean_name) };
        let path_upper = new_path.to_uppercase();

        if entry.is_dir {
            writer.create_dir(&new_path)?;
            copy_recursive(reader, writer, &new_path, tx, bytes_written, total_size, original_iso_label, new_usb_label, 
                found_kernel, found_initrd, found_args, abort_flag.clone(), use_sprout_bootloader, iso_path, 
                kernel_version_for_patch)?;
            continue;
        } 
        
        let is_new_file = WRITTEN_PATHS.with(|paths| paths.borrow_mut().insert(path_upper.clone()));
        if !is_new_file {
            let _ = tx.send(EventMsg::log(&format!("Skipping duplicate file: {}", new_path)));
            *bytes_written += entry.size;
            let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
            continue;
        }

        let stream_path = if let Some(target) = &entry.symlink_target {
            let _ = tx.send(EventMsg::log(&format!("Resolving symlink: {} -> {}", new_path, target)));
            target
        } else {
            &new_path
        };

        let _ = tx.send(EventMsg::log(&format!("Extracting: {}", new_path)));
        
        bootloader::detect_linux_payloads(&clean_name, current_path, found_kernel, found_initrd);
        
        // Look for configuration files that might contain the hardcoded 32-character ISO label
        let clean_lower = clean_name.to_lowercase();
        let is_config = clean_lower.ends_with(".cfg") 
            || clean_lower.ends_with(".conf")
            || clean_lower == "start_cfg";
        let is_hidden_efi = ["efi.img", "efiboot.img", "macefi.img"].iter()
            .any(|&name| clean_name.eq_ignore_ascii_case(name));
        let is_initrd = bootloader::LINUX_INITRAMFS_PREFIXES.iter()
            .any(|&prefix| clean_name.to_lowercase().starts_with(prefix));

        
        let is_grub_efi = clean_name.eq_ignore_ascii_case("BOOTX64.EFI") 
            || clean_name.eq_ignore_ascii_case("GRUBX64.EFI") 
            || clean_name.eq_ignore_ascii_case("BOOTAA64.EFI");

        // intercepting Initramfs
        if is_initrd && kernel_version_for_patch.is_some() {
            let _ = tx.send(EventMsg::log(&format!("Intercepting and patching initramfs: {}", new_path)));

            let (sq_offset, sq_size, actual_sq_path) = match initramfs_patcher::locate_squashfs(reader) {
                Ok(res) => res,
                Err(e) => {
                    let _ = tx.send(EventMsg::error(&e));
                    return Err("Unrecognized Linux distribution layout".to_string());
                }
            };

            let _ = tx.send(EventMsg::log(&format!("Found SquashFS at: {}", actual_sq_path)));

            let mut iso_file_handle = File::open(iso_path).map_err(|e| e.to_string())?;
            let mut virtual_reader = initramfs_patcher::SquashfsReader {
                iso_file: &mut iso_file_handle,
                squashfs_start_offset: sq_offset,
                squashfs_size: sq_size,
                current_pos: 0,
            };

            // Buffer the broken initramfs into RAM
            let mut raw_initrd = Vec::new();
            reader.stream_file(stream_path, &mut |chunk| {
                raw_initrd.extend_from_slice(chunk);
                Ok(())
            })?;

            // Fire the patcher
            let modules_to_fetch = [
                "fat.ko", "vfat.ko", "nls_cp437.ko", "nls_iso8859_1.ko", "nls_utf8.ko",
                "modules.dep.bin", "modules.alias.bin", "modules.symbols.bin",
                ];
            let kver = kernel_version_for_patch.as_deref().unwrap();
            
            let _ = tx.send(EventMsg::log("Extracting kernel modules via Virtual Bridge..."));
            let patched_bytes = initramfs_patcher::patch_initramfs(
                &raw_initrd, kver, &mut virtual_reader, &modules_to_fetch
            ).map_err(|e| format!("Initramfs Patching Failed: {}", e))?;

            // Write the healed bytes to the USB
            let mut out_file = writer.open_file_writer(&new_path, patched_bytes.len() as u64)?;
            out_file.write_all(&patched_bytes).map_err(|e| e.to_string())?;
            out_file.flush().map_err(|e| e.to_string())?;

            *bytes_written += entry.size;
            let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
            

        } else if is_grub_efi || is_hidden_efi {
            let mut file_data = Vec::new();
            reader.stream_file(stream_path, &mut |chunk| {
                file_data.extend_from_slice(chunk);
                Ok(())
            })?;

            // A valid PE32+ executables starts with 'MZ' (0x4D, 0x5A)
            let is_pe_executable = file_data.len() >= 2 && file_data[0] == 0x4D && file_data[1] == 0x5A;

            if is_pe_executable {

                // says "GRUB" but is tiny - a CD-ROM stub without the FAT32 driver
                let is_grub = file_data.windows(4).any(|w| w.eq_ignore_ascii_case(b"GRUB"));
                if is_grub && file_data.len() < 400_000 {
                    let _ = tx.send(EventMsg::log(&format!("Identified {} as a crippled CD-ROM GRUB stub. Skipping to let efi.img provide the real one.", clean_name)));
                    
                    // Remove it from the ledger so the good one in efi.img can overwrite it later!
                    WRITTEN_PATHS.with(|paths| paths.borrow_mut().remove(&path_upper)); 
                    continue; 
                }

                // A real EFI binary, try to patch it natively
                let successfully_patched = grub_patcher::patch_grub_efi(
                    &mut file_data, 
                    original_iso_label, 
                    new_usb_label
                );

                // Try to patch the hidden UUID search in the memdisk
                if grub_patcher::patch_memdisk_uuid_to_label(&mut file_data, new_usb_label) {
                    let _ = tx.send(EventMsg::log(&format!("[+] Successfully binary-patched memdisk UUID search in {}", clean_name)));
                }

                if successfully_patched {
                    let _ = tx.send(EventMsg::log(&format!("[+] Natively patched GRUB2 ({}). Disabling Sprout fallback.", clean_name)));
                    *use_sprout_bootloader = false; 
                }

                // Write the file
                let mut out_file = writer.open_file_writer(&new_path, file_data.len() as u64)?;
                out_file.write_all(&file_data).map_err(|e| e.to_string())?;
                out_file.flush().map_err(|e| e.to_string())?;

                // The UEFI Fallback Mirror
                let efi_fallback_path = format!("/EFI/BOOT/{}", clean_name.to_uppercase());
                if !new_path.eq_ignore_ascii_case(&efi_fallback_path) {
                    let is_new_mirror = WRITTEN_PATHS.with(|paths| paths.borrow_mut().insert(efi_fallback_path.clone()));
                    if is_new_mirror {
                        let _ = tx.send(EventMsg::log(&format!("Mirroring {} to strict UEFI path: {}", clean_name, efi_fallback_path)));
                        let _ = writer.create_dir_all("/EFI/BOOT");
                        
                        if let Ok(mut fb_file) = writer.open_file_writer(&efi_fallback_path, file_data.len() as u64) {
                            let _ = fb_file.write_all(&file_data);
                            let _ = fb_file.flush();
                        }
                    } else {
                        let _ = tx.send(EventMsg::log(&format!("Skipping mirror for {}, path already exists.", clean_name)));
                    }
                }
            } else {
                // a FAT image (whether named .img or disguised as .efi)
                let _ = tx.send(EventMsg::log(&format!("File {} lacks MZ magic bytes. Processing as FAT image...", clean_name)));

                // Crack it open and extract the true bootloader
                if !*use_sprout_bootloader {
                    let _ = tx.send(EventMsg::log(&format!("Intercepted hidden EFI image: {}", clean_name)));
                    if let Err(e) = inject_hidden_efi(file_data, writer, tx, new_usb_label) {                        let _ = tx.send(EventMsg::log(&format!("[-] Failed to inject hidden EFI: {}", e)));
                    } else {
                        let _ = tx.send(EventMsg::log("[+] Hidden EFI extracted to metal successfully."));
                    }
                }

                *bytes_written += entry.size;
                let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
            }
            
            *bytes_written += entry.size;
            let _ = tx.send(EventMsg::progress(*bytes_written, total_size));

        } else if is_config {

            // Buffer the config file entirely in RAM before writing so we can patch the string safely!
            let mut config_data = Vec::new();
            reader.stream_file(stream_path, &mut |chunk| {
                config_data.extend_from_slice(chunk);
                Ok(())
            })?;
            
            let final_data = if let Ok(cfg_str) = std::str::from_utf8(&config_data) {
                let patched = bootloader::patch_boot_labels(cfg_str, new_usb_label, kernel_version_for_patch.is_some());
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
        let bare_fs = fat32::BareFat32::mount(wrapped_partition, 0, 0).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to mount bare FAT32: {}", e))
        })?;
        bare_fs.inspect_all().map_err(|e| {
            PyRuntimeError::new_err(format!("Inspection failed: {}", e))
        })?
    };

    Ok(found_files)
}


