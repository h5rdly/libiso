use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::Mutex;
use ext4_lwext4::{BlockDevice, error::{Error, Result as Ext4Result}};

pub struct PartitionBlockDevice {
    file: Mutex<File>,
    offset_bytes: u64,
    size_bytes: u64,
    block_size: u32,
}

impl PartitionBlockDevice {
    pub fn new(file: File, offset_bytes: u64, size_bytes: u64) -> Self {
        Self {
            file: Mutex::new(file),
            offset_bytes,
            size_bytes,
            block_size: 4096, // Standard ext4 block size
        }
    }
}

impl BlockDevice for PartitionBlockDevice {
    fn read_blocks(&self, block_id: u64, buf: &mut [u8]) -> Ext4Result<u32> {
        let block_offset = block_id * self.block_size as u64;
        let read_offset = self.offset_bytes + block_offset;
        let block_count = buf.len() as u32 / self.block_size;

        if block_offset + buf.len() as u64 > self.size_bytes {
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Read past partition end")));
        }

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(read_offset))?;
        file.read_exact(buf)?;

        Ok(block_count)
    }

    fn write_blocks(&mut self, block_id: u64, buf: &[u8]) -> Ext4Result<u32> {
        let block_offset = block_id * self.block_size as u64;
        let write_offset = self.offset_bytes + block_offset;
        let block_count = buf.len() as u32 / self.block_size;

        if block_offset + buf.len() as u64 > self.size_bytes {
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Write past partition end")));
        }

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(write_offset))?;
        file.write_all(buf)?;

        Ok(block_count)
    }

    fn flush(&mut self) -> Ext4Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_all()?;
        Ok(())
    }

    fn block_size(&self) -> u32 {
        self.block_size
    }

    fn block_count(&self) -> u64 {
        self.size_bytes / self.block_size as u64
    }
}