// Low level controls for Windows to allow locking the USB for the eneded work


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