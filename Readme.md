![Python](https://img.shields.io/badge/Python-3.12%20%E2%80%93%203.14(t)-darkgreen?logo=python&logoColor=blue)
[![Tests](https://github.com/h5rdly/libiso/actions/workflows/tests.yml/badge.svg)](https://github.com/h5rdly/libiso/actions/workflows/tests.yml)

# libiso

libiso is Rust backed Python library for burning images on USB drives.

##  Size

The `.so` size on linux is ~`1.3Mb`, no external dependencies.


##  Installation

`pip install libiso`

- Developed on Linux / Python 3.14
- Tests are run on and wheels are available for Linux (+ Alpine) / Windows / MacOS / FreeBSD

##  Usage

```python
import libiso

for drive in libiso.list_removable_drives():
   # Summary - drive.device_path, drive.display_name, drive.total_space_bytes
   print(drive)   

# Name:  USB Flash Disk - 239 GB (/dev/sda)
# Path:  /dev/sda
# Size:  256691404800 bytes

stats = libiso.inspect_image('/path/to/manjaro-kde-26.0.4-260327-linux618.iso')
print(stats)

# Volume Label:      MANJARO_KDE_2604
# Size:              5669099520 bytes
# ISOHybrid:         true
# Large File (>4GB): false
#
# --- Boot Info ---
# Bootable:          true (BIOS: true, UEFI: true)
# Secure Boot:       false
#
# --- Windows Metadata ---
# Is Windows:        False


import tempfile, os, time 

'''
- Unix and Windows treat physical block devices as files (e.g., /dev/sdb)
- libiso's Rust writing backend is abstracted via standard Read/Write/Seek traits
- These snippets use Tempfiles for demonstartion purposes, as do the tests
'''

with (tempfile.NamedTemporaryFile(delete=False) as dest_fd, 
      tempfile.NamedTemporaryFile(delete=False) as source_fd):
   dest_fd.write(b'\x00' * (128 * 1024 * 1024)) 
   source_fd.write(os.urandom(100 * 1024 * 1024))


def show_progress(total, written, mode, sleep_interval=0.05):

   time.sleep(0.05)   # For visual effect
   percent = (written / total) * 100 if total > 0 else 0
   bar = ('█' * int(40 * written / total)).ljust(40, '-')
   print(f'\r\033[K{mode} Mode Write:  |{bar}| {percent:.1f}% ({written}/{total} bytes)', end='')

# Writing in DD mode
for written, total in libiso.write_image_dd(source_fd.name, dest_fd.name):
    show_progress(total, written, 'DD')

# DD Mode Write:  |████████████████████████████████████████| 100.0% (209715200/209715200 bytes)


'''
           ---- Writing in ISO mode  ---
- has_large_file = True makes libiso utilize FAT32 + exFAT setup
- Pulling uefi-ntfs.img
-  libiso places the UEFI bridge image int the small FAT32 partition, that then loads the actual exFAT partition with the ISO
'''

with (tempfile.NamedTemporaryFile(delete=False) as dest_fd, 
      tempfile.NamedTemporaryFile(delete=False) as source_fd):
   dest_fd.write(b'\x00' * (128 * 1024 * 1024)) 
   source_fd.write(libiso.create_mock_iso(
      'TEST_ISO', ['EFI/BOOT/BOOTX64.EFI', 'KERNEL.BIN'], True, 50))   # 50Mb for each of the 2 files

uefi_bridge_path = libiso.ensure_uefi_bridge()
for written, total in libiso.write_image_iso(
   source_fd.name, dest_fd.name, True, 'GPT', uefi_bridge_path
   ):   
   show_progress(total, written, 'ISO')

# ISO Mode Write:  |████████████████████████████████████████| 100.0% (109051904/109051904 bytes)

```

