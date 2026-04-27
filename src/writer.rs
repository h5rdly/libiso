use std::{
    fs::{File, OpenOptions},
    io::{Read, Write, Seek, SeekFrom},
    path::Path,
    thread,
    time::{SystemTime, UNIX_EPOCH},
    sync::{Arc, mpsc, Mutex, atomic::{AtomicBool, Ordering}},
    cell::RefCell
};

use hadris_part::{
    GptDisk, GptPartitionEntry, Guid, DiskPartitionScheme,
    geometry::DiskGeometry,
    mbr::{MasterBootRecord, MbrPartition, MbrPartitionType},
    scheme_io::DiskPartitionSchemeWriteExt
};

use hadris_fat::format::{FormatOptions as Fat32FormatOptions, FatVolumeFormatter, FatTypeSelection};
use fatfs::{ Dir, FileSystem, FsOptions, ReadWriteSeek, TimeProvider, OemCpConverter}; 

use hadris_iso::{sync::IsoImage, directory::DirectoryRef, read::DirEntry};

use arcbox_ext4::Formatter as Ext4Formatter;
use pyo3::prelude::*;

use crate::io::{AlignedBuffer, sys::DriveLocker, open_device, trigger_os_reread};
use crate::verify;
use crate::bootloader;
use crate::udf;
use crate::exfat::BareExFat;

pub const DD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const ISO_CHUNK_SIZE: usize = 100 * 1024;


#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct AbortToken {
    pub(crate) flag: Arc<AtomicBool>,
}

#[pymethods]
impl AbortToken {
    #[new]
    pub fn new() -> Self {
        Self { flag: Arc::new(AtomicBool::new(false)) }
    }
    pub fn abort(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }
}

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct EventMsg {
    #[pyo3(get)]
    pub msg_type: String,
    #[pyo3(get)]
    pub written: u64,
    #[pyo3(get)]
    pub total: u64,
    #[pyo3(get)]
    pub text: String,
}

impl EventMsg {
    pub fn progress(written: u64, total: u64) -> Self {
        Self { msg_type: "PROGRESS".to_string(), written, total, text: String::new() }
    }
    pub fn phase(text: &str) -> Self {
        Self { msg_type: "PHASE".to_string(), written: 0, total: 0, text: text.to_string() }
    }
    pub fn log(text: &str) -> Self {
        Self { msg_type: "LOG".to_string(), written: 0, total: 0, text: text.to_string() }
    }
    pub fn done(text: &str) -> Self {
        Self { msg_type: "DONE".to_string(), written: 0, total: 0, text: text.to_string() }
    }
    pub fn error(text: &str) -> Self {
        Self { msg_type: "ERROR".to_string(), written: 0, total: 0, text: text.to_string() }
    }
}

#[pyclass]
pub struct ProgressStream {
    pub(crate) rx: Mutex<mpsc::Receiver<EventMsg>>,
}

#[pymethods]
impl ProgressStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }

    fn __next__(&self, py: Python) -> PyResult<Option<EventMsg>> {
        match py.detach(|| self.rx.lock().unwrap().recv()) {
            Ok(msg) => Ok(Some(msg)),
            Err(_) => Ok(None), 
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
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "Partition boundary exceeded")); 
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

fn pseudo_uuid() -> [u8; 16] {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let s = t.as_secs();
    let n = t.subsec_nanos();
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&s.to_le_bytes());
    bytes[8..12].copy_from_slice(&n.to_le_bytes());
    let mix = (s as u32) ^ n;
    bytes[12..16].copy_from_slice(&mix.to_le_bytes());
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    bytes
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

pub fn format_partition<T: Read + Write + Seek>(
    wrapped_partition: &mut PartitionWrapper<T>,
    is_exfat: bool,
    volume_label: &str,
    start_lba: u64,
) -> Result<(), String> {
    let sys_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let vol_id = (sys_time & 0xFFFFFFFF) as u32;
    wrapped_partition.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    let part_size = wrapped_partition.size;

    if is_exfat {
        let mut options = hadris_fat::exfat::ExFatFormatOptions::new().with_label(volume_label);
        options.partition_offset = start_lba as u64; 
        hadris_fat::exfat::format_exfat(&mut *wrapped_partition, part_size, &options)
            .map_err(|e| format!("exFAT Format failed: {:?}", e))?;
    } else {
        let options = Fat32FormatOptions::new(part_size)
            .with_label(volume_label)
            .with_fat_type(FatTypeSelection::Fat32)
            .with_volume_id(vol_id)
            .with_hidden_sectors(start_lba as u32);
        FatVolumeFormatter::format(&mut *wrapped_partition, options)
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
    let _locker = DriveLocker::new(&device_path).map_err(|e| pyo3::exceptions::PyPermissionError::new_err(e))?;
    let mut dest_file = OpenOptions::new().read(true).write(true).open(&device_path)?;
    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    if dest_size == 0 { return Err(pyo3::exceptions::PyIOError::new_err("Device has 0 bytes.")); }

    let total_sectors = dest_size / 512;
    let scheme = partition_scheme.unwrap_or_else(|| "gpt".to_string());
    if total_sectors > 0xFFFF_FFFF && scheme.eq_ignore_ascii_case("mbr") {
        return Err(pyo3::exceptions::PyValueError::new_err("Drive is too large (> 2TB) for standard MBR formatting."));
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

    let geom = DiskGeometry::standard(total_sectors);
    let start_lba = geom.gpt_first_usable_lba_aligned(128, 128); 
    let safe_end_lba = geom.gpt_last_usable_lba_aligned(128, 128); 

    if scheme.eq_ignore_ascii_case("mbr") {
        let mut mbr = MasterBootRecord::default();
        mbr.with_partition_table(|pt| {
            let p_type = if is_exfat { MbrPartitionType::Ntfs } else { MbrPartitionType::Fat32Lba };
            pt[0] = MbrPartition::new(p_type, start_lba as u32, (safe_end_lba - start_lba + 1) as u32);
            pt[0].set_bootable(true);
        });
        let scheme = DiskPartitionScheme::Mbr(mbr);
        DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to write MBR: {:?}", e)))?;
    } else {
        let mut gpt = GptDisk::new(total_sectors, 512);
        let part1 = GptPartitionEntry::new(Guid::BASIC_DATA, Guid::from_bytes(pseudo_uuid()), start_lba, safe_end_lba);
        gpt.add_partition(part1).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{:?}", e)))?;
        let protective_mbr = gpt.create_protective_mbr();
        let scheme = DiskPartitionScheme::Gpt { protective_mbr, gpt };
        DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to write GPT: {:?}", e)))?;
    }
    dest_file.sync_all()?;

    let partition_offset = start_lba * 512;
    let partition_size = (safe_end_lba - start_lba + 1) * 512;
    let mut wrapped_partition = PartitionWrapper { inner: dest_file, offset: partition_offset, size: partition_size };
    format_partition(&mut wrapped_partition, is_exfat, volume_label, start_lba).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;

    Ok(())
}

#[pyfunction]
#[pyo3(signature = (image_path, device_path, verify_written=false, abort_token=None))]
pub fn write_image_dd(
    image_path: String, device_path: String, verify_written: bool, abort_token: Option<PyRef<'_, AbortToken>>,
) -> PyResult<ProgressStream> {
    let iso_path = Path::new(&image_path);
    if !iso_path.exists() { return Err(pyo3::exceptions::PyFileNotFoundError::new_err("Image not found")); }
    let total_size = iso_path.metadata()?.len();

    let mut dest_file = OpenOptions::new().write(true).open(&device_path)
        .map_err(|e| pyo3::exceptions::PyPermissionError::new_err(format!("Open device err: {}", e)))?;
    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    dest_file.seek(SeekFrom::Start(0))?; 
    if total_size > dest_size {
        return Err(pyo3::exceptions::PyValueError::new_err(format!("Target device ({} bytes) is too small for this image ({} bytes).", dest_size, total_size)));
    }
    drop(dest_file);

    let (tx, rx) = mpsc::sync_channel::<EventMsg>(100);
    let abort_flag = abort_token.map(|t| t.flag.clone()).unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    thread::spawn(move || {
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
    unattend_xml_payload=None, target_arch=None, abort_token=None))]
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
    abort_token: Option<PyRef<'_, AbortToken>> 
) -> PyResult<ProgressStream> {
    
    let arch_selection = target_arch.unwrap_or_else(|| "all".to_string());
    let scheme = partition_scheme.unwrap_or_else(|| "mbr".to_string());

    let iso_path = Path::new(&image_path);
    if !iso_path.exists() { return Err(pyo3::exceptions::PyFileNotFoundError::new_err("ISO not found")); }

    let total_size = iso_path.metadata()?.len();
    let _locker = DriveLocker::new(&device_path).map_err(|e| pyo3::exceptions::PyPermissionError::new_err(e))?;
    let mut dest_file = OpenOptions::new().read(true).write(true).open(&device_path)
        .map_err(|e| pyo3::exceptions::PyPermissionError::new_err(format!("Failed to open device '{}'. Error: {}", device_path, e)))?;

    let dest_size = dest_file.seek(SeekFrom::End(0))?;
    dest_file.seek(SeekFrom::Start(0))?; 

    if dest_size == 0 { return Err(pyo3::exceptions::PyIOError::new_err("Destination device has 0 bytes.")); }

    if total_size > dest_size {
        return Err(pyo3::exceptions::PyValueError::new_err(format!("Target device ({} bytes) is too small for this image ({} bytes).", dest_size, total_size)));
    }

    let total_sectors = dest_size / 512;
    if total_sectors > 0xFFFF_FFFF && scheme.eq_ignore_ascii_case("mbr") {
        return Err(pyo3::exceptions::PyValueError::new_err("MBR partition scheme does not support drives larger than 2TB."));
    }

    let geom = DiskGeometry::standard(total_sectors);
    let start_lba = geom.gpt_first_usable_lba_aligned(128, 128); 
    let safe_end_lba = geom.gpt_last_usable_lba_aligned(128, 128);

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

    if scheme.eq_ignore_ascii_case("mbr") {
        let mut mbr = MasterBootRecord::default();
        mbr.with_partition_table(|pt| {
            let p_type = if has_large_file { MbrPartitionType::Ntfs } else { MbrPartitionType::Fat32Lba }; 
            pt[0] = MbrPartition::new(p_type, start_lba as u32, part1_sectors as u32);
            pt[0].set_bootable(true);
            if let Some((start, size)) = efi_part { pt[1] = MbrPartition::new(MbrPartitionType::EfiSystemPartition, start as u32, size as u32); }
            if let Some((start, size)) = persistence_part { pt[2] = MbrPartition::new(MbrPartitionType::LinuxNative, start as u32, size as u32); }
        });

        let scheme = DiskPartitionScheme::Mbr(mbr);
        DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to write MBR: {:?}", e)))?;
    } else {
        let mut gpt = GptDisk::new(total_sectors, 512);
        let disk_id = Guid::from_bytes(pseudo_uuid());
        gpt.primary_header.disk_guid = disk_id;
        gpt.backup_header.disk_guid = disk_id;

        let part1 = GptPartitionEntry::new(Guid::BASIC_DATA, Guid::from_bytes(pseudo_uuid()), start_lba, part1_end_lba);
        gpt.add_partition(part1).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Part 1 err: {:?}", e)))?;
        if let Some((start, size)) = efi_part {
            let part2 = GptPartitionEntry::new(Guid::EFI_SYSTEM, Guid::from_bytes(pseudo_uuid()), start, start + size - 1);
            gpt.add_partition(part2).unwrap();
        }
        if let Some((start, size)) = persistence_part {
            let part3 = GptPartitionEntry::new(Guid::LINUX_FILESYSTEM, Guid::from_bytes(pseudo_uuid()), start, start + size - 1);
            gpt.add_partition(part3).unwrap();
        }

        let protective_mbr = gpt.create_protective_mbr();
        let scheme = DiskPartitionScheme::Gpt { protective_mbr, gpt };
        DiskPartitionSchemeWriteExt::write_to(&scheme, &mut dest_file).map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("Failed to write GPT: {:?}", e)))?;
    }
    dest_file.sync_all()?;

    let mut lba0_bytes = [0u8; 512];
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.read_exact(&mut lba0_bytes)?;
    dest_file.seek(SeekFrom::Start(0))?;
    dest_file.write_all(&[0u8; 512])?;
    dest_file.sync_all()?;

    if let Some((start, sectors)) = persistence_part {
        let ext4_offset = start * 512;
        let ext4_size = sectors * 512;
        
        let temp_path_str = ext4_temp_path.ok_or_else(|| pyo3::exceptions::PyValueError::new_err("ext4_temp_path must be provided if persistence_size_mb is set"))?;
        let temp_ext4 = Path::new(&temp_path_str);
        let fmt = Ext4Formatter::new(temp_ext4, 4096, ext4_size).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to init ext4 formatter: {:?}", e)))?;
        fmt.close().map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to finalize ext4 formatting: {:?}", e)))?;

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

    let mut dest_file_for_uefi = dest_file.try_clone()?;

    let mut wrapped_partition = PartitionWrapper {
        inner: dest_file,
        offset: partition_offset_bytes,
        size: partition_size_bytes,
    };

    let short_label = iso_label.chars().take(11).collect::<String>().replace(' ', "_").to_uppercase();
    format_partition(&mut wrapped_partition, has_large_file, &short_label, start_lba)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
    
    let iso_path_clone = image_path.clone();

    let (tx, rx) = mpsc::sync_channel::<EventMsg>(100);
    let abort_flag = abort_token.map(|t| t.flag.clone()).unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    let autorun_inf_label = iso_label.to_string();
    let device_path_clone = device_path.clone();

    // The UEFI bridge copying remains outside the thread so it can return permission errors early
    if has_large_file {
        let uefi_path = uefi_ntfs_path.ok_or_else(|| pyo3::exceptions::PyValueError::new_err("uefi_ntfs_path must be provided for large files (exFAT mode)"))?;
        let mut uefi_file = File::open(&uefi_path)?;
        if let Some((start, _)) = efi_part {
            dest_file_for_uefi.seek(SeekFrom::Start(start * 512))?;
            std::io::copy(&mut uefi_file, &mut dest_file_for_uefi)?;
            dest_file_for_uefi.sync_all()?;
        }
    }

    thread::spawn(move || {
        let mut wp = wrapped_partition;
        wp.seek(SeekFrom::Start(0)).unwrap();

        if has_large_file {
            let _ = tx.send(EventMsg::phase("Extracting image (exFAT Bare-Metal)"));
            
            // USE OUR NEW ZERO-DEPENDENCY BARE METAL WRITER!
            let bare_fs = match BareExFat::mount(wp) {
                Ok(fs) => fs,
                Err(e) => { let _ = tx.send(EventMsg::error(&format!("Failed to mount Bare exFAT: {}", e))); return; }
            };
            
            run_burn_and_verify(
                &bare_fs, &bare_fs, &iso_path_clone, &device_path_clone, &tx, total_size, 
                has_large_file, &arch_selection, unattend_xml_payload, &autorun_inf_label, 
                abort_flag, verify_written, lba0_bytes
            );
            
        } else {
            let _ = tx.send(EventMsg::phase("Extracting image (FAT32)"));
            let fatfs_partition = fatfs::StdIoWrapper::new(wp);
            let usb_fs = match FileSystem::new(fatfs_partition, FsOptions::new()) {
                Ok(fs) => fs,
                Err(e) => { let _ = tx.send(EventMsg::error(&format!("Failed to mount FAT32: {:?}", e))); return; }
            };
            
            let writer = Fat32Writer { fs: &usb_fs };
            let usb_reader = verify::Fat32UsbReader { fs: &usb_fs };
            
            run_burn_and_verify(
                &writer, &usb_reader, &iso_path_clone, &device_path_clone, &tx, total_size, 
                has_large_file, &arch_selection, unattend_xml_payload, &autorun_inf_label, 
                abort_flag, verify_written, lba0_bytes
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
}

pub trait ImageReader {
    fn list_dir(&self, path: &str) -> Result<Vec<ImageNode>, String>;
    fn stream_file(&self, path: &str, on_chunk: &mut dyn FnMut(&[u8]) -> Result<(), String>) -> Result<(), String>;
}

pub trait UsbWriter {
    type FileWriter<'w>: Write + 'w where Self: 'w;
    fn create_dir(&self, path: &str) -> Result<(), String>;
    fn open_file_writer<'w>(&'w self, path: &str, size: u64) -> Result<Self::FileWriter<'w>, String>;
}


// --- ISO9660 READER ---
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
}
impl<'a> ImageReader for IsoReader<'a> {
    fn list_dir(&self, path: &str) -> Result<Vec<ImageNode>, String> {
        let dir_ref = self.resolve_dir(path)?;
        let mut nodes = Vec::new();
        for entry in self.iso.open_dir(dir_ref).entries().filter_map(Result::ok) {
            let name = get_clean_filename(&entry);
            if name == "\x00" || name == "\x01" || name == "." || name == ".." { continue; }
            nodes.push(ImageNode { name, is_dir: entry.is_directory(), size: entry.total_size() as u64 });
        }
        Ok(nodes)
    }
    fn stream_file(&self, path: &str, on_chunk: &mut dyn FnMut(&[u8]) -> Result<(), String>) -> Result<(), String> {
        let parent_path = if let Some(idx) = path.rfind('/') { &path[..idx] } else { "" };
        let file_name = if let Some(idx) = path.rfind('/') { &path[idx+1..] } else { path };
        
        let parent_ref = self.resolve_dir(parent_path)?;
        let mut target_entry = None;
        for entry in self.iso.open_dir(parent_ref).entries().filter_map(Result::ok) {
            if get_clean_filename(&entry).eq_ignore_ascii_case(file_name) && !entry.is_directory() {
                target_entry = Some(entry);
                break;
            }
        }
        let entry = target_entry.ok_or_else(|| format!("File not found: {}", file_name))?;
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

// --- UDF READER ---
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
            
            nodes.push(ImageNode { name, is_dir: e.is_directory, size });
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


fn execute_extraction_workflow<R: ImageReader, W: UsbWriter>(
    reader: &R,
    writer: &W,
    tx: &mpsc::SyncSender<EventMsg>,
    total_size: u64,
    has_large_file: bool,
    arch_selection: &str,
    unattend_xml_payload: Option<String>,
    autorun_inf_label: &str,
    abort_flag: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut written = 0u64;
    let mut found_kernel = None;
    let mut found_initrd = None;
    let mut found_args = None;

    // Extract Files
    copy_recursive(
        reader, writer, "", tx, &mut written, total_size, 
        &mut found_kernel, &mut found_initrd, &mut found_args, abort_flag.clone()
    )?;

    // Linux Sprout Bootloader (Only if not a Windows ISO)
    if !has_large_file {
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
    unattend_xml_payload: Option<String>,
    autorun_inf_label: &str,
    abort_flag: Arc<AtomicBool>,
    verify_written: bool,
    lba0_bytes: [u8; 512], 
) {
    let mut file = File::open(iso_path).unwrap();
    let is_udf_valid = if let Ok(udf_ctx) = crate::udf::mount_udf(&mut file) {
        crate::udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
    } else { false };

    let extract_res = if is_udf_valid {
        let _ = tx.send(EventMsg::log("Using UDF Parser"));
        let udf_ctx = crate::udf::mount_udf(&mut file).unwrap();
        let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
        execute_extraction_workflow(
            &reader, writer, tx, total_size, has_large_file, 
            arch_selection, unattend_xml_payload, autorun_inf_label, abort_flag
        )
    } else {
        let _ = tx.send(EventMsg::log("Using ISO9660 Parser"));
        let iso_file = File::open(iso_path).unwrap();
        let iso = IsoImage::open(iso_file).unwrap();
        let reader = IsoReader { iso: &iso };
        execute_extraction_workflow(
            &reader, writer, tx, total_size, has_large_file, 
            arch_selection, unattend_xml_payload, autorun_inf_label, abort_flag
        )
    };

    if let Err(e) = extract_res {
        let _ = tx.send(EventMsg::error(&format!("Extraction error: {}", e)));
        return;
    }

     // Flush the OS cache
    if let Ok(mut f_reread) = OpenOptions::new().read(true).write(true).open(device_path) {
        f_reread.seek(SeekFrom::Start(0)).unwrap();
        f_reread.write_all(&lba0_bytes).unwrap();
        f_reread.sync_all().unwrap();

        if let Err(e) = trigger_os_reread(&f_reread, device_path) {
            let _ = tx.send(EventMsg::log(&format!("OS cache flush warning: {}", e)));
        } else {
            let _ = tx.send(EventMsg::log("OS cache flushed successfully."));
        }
    }

    if verify_written {
        let mut file = File::open(iso_path).unwrap();
        let verify_res = if is_udf_valid {
            let udf_ctx = crate::udf::mount_udf(&mut file).unwrap();
            let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
            execute_verify_workflow(&reader, usb_reader, tx, total_size, !has_large_file)
        } else {
            let iso_file = File::open(iso_path).unwrap();
            let iso = IsoImage::open(iso_file).unwrap();
            let reader = IsoReader { iso: &iso };
            execute_verify_workflow(&reader, usb_reader, tx, total_size, !has_large_file)
        };
        if let Err(e) = verify_res {
            let _ = tx.send(EventMsg::error(&format!("Verification error: {}", e)));
            return;
        }
    }

    let msg = if verify_written { "ISO Burn and Verify Complete!" } else { "ISO Burn Complete!" };
    let _ = tx.send(EventMsg::done(msg));
}


// --- FAT32 WRITER ---
pub struct Fat32Writer<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> { pub fs: &'a FileSystem<T, TP, OCC> }
pub struct Fat32FileWriter<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> { inner: fatfs::File<'a, T, TP, OCC> }
impl<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> Write for Fat32FileWriter<'a, T, TP, OCC> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        fatfs::Write::write(&mut self.inner, buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        fatfs::Write::flush(&mut self.inner).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))
    }
}
impl<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> Fat32Writer<'a, T, TP, OCC> {
    fn nav(&self, path: &str) -> Result<(Dir<'a, T, TP, OCC>, String), String> {
        let mut curr = self.fs.root_dir();
        let mut parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let target = parts.pop().unwrap_or("").to_string();
        for p in parts { if !p.is_empty() { curr = curr.open_dir(p).map_err(|e| format!("{:?}", e))?; } }
        Ok((curr, target))
    }
}
impl<'a, T: ReadWriteSeek, TP: TimeProvider, OCC: OemCpConverter> UsbWriter for Fat32Writer<'a, T, TP, OCC> {
    type FileWriter<'w> = Fat32FileWriter<'w, T, TP, OCC> where Self: 'w;
    fn create_dir(&self, path: &str) -> Result<(), String> {
        let (parent, name) = self.nav(path)?;
        parent.create_dir(&name).map_err(|e| format!("{:?}", e))?;
        Ok(())
    }
    fn open_file_writer<'w>(&'w self, path: &str, _size: u64) -> Result<Self::FileWriter<'w>, String> {
        let (parent, name) = self.nav(path)?;
        let mut f = parent.create_file(&name).map_err(|e| format!("{:?}", e))?;
        f.truncate().map_err(|e| format!("{:?}", e))?; 
        Ok(Fat32FileWriter { inner: f })
    }
}


pub fn copy_recursive<R: ImageReader, W: UsbWriter>(
    reader: &R,
    writer: &W,
    current_path: &str,
    tx: &mpsc::SyncSender<EventMsg>,
    bytes_written: &mut u64,
    total_size: u64,
    found_kernel: &mut Option<String>,  
    found_initrd: &mut Option<String>,  
    found_args: &mut Option<String>, 
    abort_flag: Arc<AtomicBool>,
) -> Result<(), String> {
    
    let entries = reader.list_dir(current_path)?;
    
    for entry in entries {
        if abort_flag.load(Ordering::Relaxed) { return Err("Cancelled by user".into()); }

        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { continue; }
        
        let new_path = if current_path.is_empty() { format!("/{}", clean_name) } else { format!("{}/{}", current_path, clean_name) };
        
        if entry.is_dir {
            writer.create_dir(&new_path)?;
            copy_recursive(reader, writer, &new_path, tx, bytes_written, total_size, found_kernel, found_initrd, found_args, abort_flag.clone())?;
        } else {
            let _ = tx.send(EventMsg::log(&format!("Extracting: {}", new_path)));
            let mut out_file = writer.open_file_writer(&new_path, entry.size)?;
            
            bootloader::detect_linux_payloads(&clean_name, current_path, found_kernel, found_initrd);
            
            let is_config = clean_name.to_lowercase().ends_with(".cfg");
            let mut config_data = Vec::new();
            
            reader.stream_file(&new_path, &mut |chunk| {
                if is_config { config_data.extend_from_slice(chunk); }
                out_file.write_all(chunk).map_err(|e| e.to_string())?;
                *bytes_written += chunk.len() as u64;
                let _ = tx.send(EventMsg::progress(*bytes_written, total_size));
                Ok(())
            })?;
            
            if is_config {
                if let Ok(cfg_str) = std::str::from_utf8(&config_data) {
                    bootloader::scrape_boot_args(cfg_str, found_args);
                }
            }
            out_file.flush().map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}


// -- Helper inspection logic and util for Python

fn inspect_fat32_recursive<IO: fatfs::ReadWriteSeek, TP: fatfs::TimeProvider, OCC: fatfs::OemCpConverter>(
    dir: &fatfs::Dir<'_, IO, TP, OCC>,
    current_path: &str,
    found_files: &mut Vec<String>,
) {
    for entry_res in dir.iter() {
        if let Ok(entry) = entry_res {
            let short = entry.short_file_name();
            if short == "." || short == ".." { continue; }

            let long = entry.file_name();
            let name = if long.is_empty() { short.clone() } else { long };
            let full_path = if current_path.is_empty() { name.clone() } else { format!("{}/{}", current_path, name) };

            if entry.is_dir() {
                found_files.push(format!("[DIR ] {}", full_path));
                if let Ok(sub_dir) = dir.open_dir(&name) {
                    inspect_fat32_recursive(&sub_dir, &full_path, found_files);
                }
            } else {
                found_files.push(format!("[FILE] {} ({} bytes)", full_path, entry.len()));
            }
        }
    }
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

    wrapped_partition.seek(SeekFrom::Start(3))?;
    let mut sig = [0u8; 8];
    wrapped_partition.read_exact(&mut sig)?;
    let is_exfat = &sig == b"EXFAT   ";
    
    wrapped_partition.seek(SeekFrom::Start(0))?;

    let mut found_files = Vec::new();

    if is_exfat {
        let bare_fs = BareExFat::mount(wrapped_partition).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to mount bare exFAT: {}", e))
        })?;
        found_files = bare_fs.inspect_all().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Inspection failed: {}", e))
        })?;
    } else {
        let fatfs_partition = fatfs::StdIoWrapper::new(wrapped_partition);
        let fs = FileSystem::new(fatfs_partition, FsOptions::new()).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to mount FAT32: {:?}", e))
        })?;
        inspect_fat32_recursive(&fs.root_dir(), "", &mut found_files);
    }

    Ok(found_files)
}


fn extract_to_fs<R: ImageReader>(reader: &R, current_path: &str, host_dir: &Path,
) -> Result<(), String> {

    let entries = reader.list_dir(current_path)?;

    for entry in entries {
        let clean_name = entry.name;
        if clean_name == "." || clean_name == ".." || clean_name.is_empty() { 
            continue; 
        }

        let new_img_path = if current_path.is_empty() { 
            format!("/{}", clean_name) 
        } else { 
            format!("{}/{}", current_path, clean_name) 
        };
        
        let new_host_path = host_dir.join(&clean_name);

        if entry.is_dir {
            std::fs::create_dir_all(&new_host_path).map_err(|e| e.to_string())?;
            extract_to_fs(reader, &new_img_path, &new_host_path)?;
        } else {
            let mut out_file = File::create(&new_host_path).map_err(|e| e.to_string())?;
            reader.stream_file(&new_img_path, &mut |chunk| {
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
        pyo3::exceptions::PyIOError::new_err(format!("Failed to open ISO: {}", e))
    })?;

    let is_udf_valid = if let Ok(udf_ctx) = crate::udf::mount_udf(&mut file) {
        crate::udf::read_directory(&mut file, udf_ctx.partition_start, &udf_ctx.root_icb).is_ok()
    } else { 
        false 
    };

    if is_udf_valid {
        let udf_ctx = crate::udf::mount_udf(&mut file).unwrap();
        let reader = UdfReader { file: RefCell::new(&mut file), ctx: &udf_ctx };
        extract_to_fs(&reader, "", host_root).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(e)
        })?;
    } else {
        let iso_file = File::open(&image_path)?;
        let iso = IsoImage::open(iso_file).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to parse ISO9660: {:?}", e))
        })?;
        let reader = IsoReader { iso: &iso };
        extract_to_fs(&reader, "", host_root).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(e)
        })?;
    }

    Ok(())
}

