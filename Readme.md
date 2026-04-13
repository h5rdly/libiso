![Python](https://img.shields.io/badge/Python-3.12%20%E2%80%93%203.14(t)-darkgreen?logo=python&logoColor=blue)
[![Tests](https://github.com/h5rdly/libiso/actions/workflows/tests.yml/badge.svg)](https://github.com/h5rdly/libiso/actions/workflows/tests.yml)

# libiso

libiso is Rust backed Python library for burning images on USB drives.

##  Size

The `.so` size on linux is ~`0.6Mb`, no external dependencies.


##  Installation

`pip install libiso`

- Developed on Linux / Python 3.14
- Tests are run on and wheels are available for Linux (+ Alpine) / Windows / MacOS / FreeBSD

##  Usage


```python
import libiso

for drive in libiso.list_removable_drives():
   print(f"Name:  {drive.display_name}")
   print(f"Path:  {drive.device_path}")
   print(f"Size:  {drive.total_space_bytes} bytes\n")

# Name:  USB Flash Disk - 239 GB (/dev/sda)
# Path:  /dev/sda
# Size:  256691404800 bytes

stats = libiso.inspect_image('/path/to/manjaro-kde-26.0.4-260327-linux618.iso'
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



```
