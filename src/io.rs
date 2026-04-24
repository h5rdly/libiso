use std::ops::{Deref, DerefMut};
use std::fs::{File, OpenOptions};
use std::path::Path;

// -- A page-aligned memory buffer for bypassing the OS cache

#[allow(dead_code)]    // Dead code analyzer doesn't pick up unsafe pointer casts
#[repr(align(4096))]
#[derive(Clone)]
struct Align4K([u8; 4096]);

pub struct AlignedBuffer {
    _storage: Vec<Align4K>,
    len: usize,
}

impl AlignedBuffer {
    // The actual memory allocated will be rounded up to the nearest 4096 bytes.
    pub fn new(capacity_bytes: usize) -> Self {
        // ensure we always allocate enough 4K chunks to fit the requested bytes
        let chunks = capacity_bytes.div_ceil(4096);
        Self {
            _storage: vec![Align4K([0u8; 4096]); chunks],
            len: capacity_bytes,
        }
    }
}

// Traits to allow the struct to pass as a standard byte slice
impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::slice::from_raw_parts(
                self._storage.as_ptr() as *const u8,
                self.len,
            )
        }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::slice::from_raw_parts_mut(
                self._storage.as_mut_ptr() as *mut u8,
                self.len,
            )
        }
    }
}



// -- Low level controls for Windows to allow locking the USB for the eneded work


#[cfg(not(windows))]
pub mod sys {
    pub struct DriveLocker {}

    impl DriveLocker {
        pub fn new(_volume_path: &str) -> Result<Self, String> {
            // Unix systems generally respect raw block writes if running as root
            Ok(Self {})
        }
    }
}


#[cfg(windows)]
pub mod sys {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use std::os::raw::c_void;

    // Minimal Win32 FFI Definitions
    type HANDLE = *mut c_void;
    type DWORD = u32;
    type BOOL = i32;
    type LPCWSTR = *const u16;
    type LPVOID = *mut c_void;

    const INVALID_HANDLE_VALUE: HANDLE = -1isize as HANDLE;
    const GENERIC_READ: DWORD = 0x80000000;
    const GENERIC_WRITE: DWORD = 0x40000000;
    const FILE_SHARE_READ: DWORD = 0x00000001;
    const FILE_SHARE_WRITE: DWORD = 0x00000002;
    const OPEN_EXISTING: DWORD = 3;

    const FSCTL_LOCK_VOLUME: DWORD = 0x00090018;
    const FSCTL_UNLOCK_VOLUME: DWORD = 0x0009001C;
    const FSCTL_DISMOUNT_VOLUME: DWORD = 0x00090020;


    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateFileW(
            lpFileName: LPCWSTR,
            dwDesiredAccess: DWORD,
            dwShareMode: DWORD,
            lpSecurityAttributes: LPVOID,
            dwCreationDisposition: DWORD,
            dwFlagsAndAttributes: DWORD,
            hTemplateFile: HANDLE,
        ) -> HANDLE;

        fn DeviceIoControl(
            hDevice: HANDLE,
            dwIoControlCode: DWORD,
            lpInBuffer: LPVOID,
            nInBufferSize: DWORD,
            lpOutBuffer: LPVOID,
            nOutBufferSize: DWORD,
            lpBytesReturned: *mut DWORD,
            lpOverlapped: LPVOID,
        ) -> BOOL;

        fn CloseHandle(hObject: HANDLE) -> BOOL;
    }
    // -------------------------------------

    pub struct DriveLocker {
        // 'None' if it's just a file.
        handle: Option<HANDLE>,
    }

    impl DriveLocker {
        pub fn new(volume_path: &str) -> Result<Self, String> {
            // If the path doesn't start with the Windows device namespace (\\.\),
            // it's a standard file - skip locking
            if !volume_path.starts_with("\\\\.\\") {
                return Ok(Self { handle: None });
            }

            // Convert Rust string to Windows UTF-16 wide string
            let wide_path: Vec<u16> = OsStr::new(volume_path)
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();

            // Get a handle to the volume/device
            let handle = unsafe {
                CreateFileW(
                    wide_path.as_ptr(),
                    GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    std::ptr::null_mut(),
                    OPEN_EXISTING,
                    0,
                    std::ptr::null_mut(),
                )
            };

            if handle == INVALID_HANDLE_VALUE {
                return Err(format!("Failed to open device '{}' for locking. Are you running as Administrator?", volume_path));
            }

            let mut bytes_returned = 0;

            let lock_success = unsafe {
                DeviceIoControl(
                    handle,
                    FSCTL_LOCK_VOLUME,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    0,
                    &mut bytes_returned,
                    std::ptr::null_mut(),
                )
            };

            if lock_success == 0 {
                unsafe { CloseHandle(handle) };
                return Err(format!("Failed to acquire FSCTL_LOCK_VOLUME on {}. Ensure no other programs are using the drive.", volume_path));
            }

            let dismount_success = unsafe {
                DeviceIoControl(
                    handle,
                    FSCTL_DISMOUNT_VOLUME,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    0,
                    &mut bytes_returned,
                    std::ptr::null_mut(),
                )
            };

            if dismount_success == 0 {
                println!("Warning: FSCTL_DISMOUNT_VOLUME failed, but lock was acquired.");
            }

            Ok(Self { handle: Some(handle) })
        }
    }

    impl Drop for DriveLocker {
        fn drop(&mut self) {
            // Only attempt to unlock and close if we actually locked a raw device!
            if let Some(h) = self.handle {
                let mut bytes_returned = 0;
                unsafe {
                    DeviceIoControl(
                        h,
                        FSCTL_UNLOCK_VOLUME,
                        std::ptr::null_mut(),
                        0,
                        std::ptr::null_mut(),
                        0,
                        &mut bytes_returned,
                        std::ptr::null_mut(),
                    );
                    CloseHandle(h);
                }
            }
        }
    }
}


// Linux O_DIRECT Constants 
#[cfg(target_os = "linux")]
mod linux_sys {
    // x86_64, x86, aarch64, arm, riscv64, powerpc, loongarch64 use 0x4000
    #[cfg(not(any(
        target_arch = "mips", 
        target_arch = "mips64", 
        target_arch = "sparc", 
        target_arch = "sparc64"
    )))]
    pub const O_DIRECT: i32 = 0x4000;

    // MIPS uses 0x8000
    #[cfg(any(target_arch = "mips", target_arch = "mips64"))]
    pub const O_DIRECT: i32 = 0x8000;

    // SPARC uses 0x100000
    #[cfg(any(target_arch = "sparc", target_arch = "sparc64"))]
    pub const O_DIRECT: i32 = 0x100000;
}


// Attempt Unbuffered I/O, fall back to standard buffered I/O if the filesystem rejects it
pub fn open_device(path_str: &str, write_access: bool) -> std::io::Result<File> {
    let path = Path::new(path_str);
    let mut opts = OpenOptions::new();
    
    opts.read(true);
    if write_access {
        opts.write(true);
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        let mut unbuf_opts = opts.clone();
        
        // FILE_FLAG_NO_BUFFERING (0x20000000) | FILE_FLAG_WRITE_THROUGH (0x80000000)
        unbuf_opts.custom_flags(0x20000000 | 0x80000000); 
        
        match unbuf_opts.open(path) {
            Ok(f) => return Ok(f),
            // Error 87: ERROR_INVALID_PARAMETER (Filesystem doesn't support no-buffering, e.g., a Python temp file)
            Err(e) if e.raw_os_error() == Some(87) => {
                println!("Warning: Unbuffered I/O rejected by OS. Falling back to cached I/O.");
            }
            Err(e) => return Err(e),
        }
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut unbuf_opts = opts.clone();
        
        unbuf_opts.custom_flags(linux_sys::O_DIRECT);
        
        match unbuf_opts.open(path) {
            Ok(f) => return Ok(f),
            // Error 22: EINVAL (Tmpfs or filesystem doesn't support O_DIRECT)
            Err(e) if e.raw_os_error() == Some(22) => {
                // Silently fallback so unit tests on /tmp don't break
            }
            Err(e) => return Err(e),
        }
    }

    // --- Mac OS / Fallback ---
    // (Mac uses F_NOCACHE via fcntl instead of open flags, so we just use standard I/O for now)
    opts.open(path)
}



#[cfg(target_os = "linux")]
pub fn trigger_os_reread(file: &std::fs::File) -> std::io::Result<()> {

    use std::os::unix::io::AsRawFd;
    use std::thread;
    use std::time::Duration;
    use std::ffi::{c_int, c_ulong};

    unsafe extern "C" {
        fn ioctl(fd: c_int, request: c_ulong, ...) -> c_int;
    }

    const BLKRRPART: c_ulong = 0x125F; 
    let _ = file.sync_all();
    thread::sleep(Duration::from_millis(500));
    
    // Tell the Linux kernel to drop its cache and re-read the partition map 
    unsafe {
        let res = ioctl(file.as_raw_fd(), BLKRRPART);
        if res != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn trigger_os_reread(_file: &std::fs::File) {
    // macOS and Windows auto-discovery r
}