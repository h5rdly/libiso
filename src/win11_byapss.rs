#![windows_subsystem = "windows"] // Prevents the command prompt window from showing

use std::env;
use std::path::Path;
use std::os::raw::c_void;
use std::process::{Command, exit};


//  Minimal Win32 FFI Definitions ---
type HKEY = *mut c_void;
type LPCWSTR = *const u16;
type LPBYTE = *const u8;
type DWORD = u32;
type LSTATUS = i32;
type REGSAM = u32;
type LPSECURITY_ATTRIBUTES = *mut c_void;

const HKEY_LOCAL_MACHINE: HKEY = -2147483646isize as HKEY;
const KEY_ALL_ACCESS: REGSAM = 983103;
const REG_OPTION_NON_VOLATILE: DWORD = 0;
const REG_MULTI_SZ: DWORD = 7;
const REG_DWORD: DWORD = 4;
const ERROR_SUCCESS: LSTATUS = 0;


// Helper to convert standard Rust strings to Windows UTF-16 null-terminated strings
fn to_utf16(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}


// Link against the Advanced API Windows library
#[link(name = "advapi32")]
extern "system" {
    fn RegDeleteTreeW(hKey: HKEY, lpSubKey: LPCWSTR) -> LSTATUS;
    
    fn RegCreateKeyExW(
        hKey: HKEY,
        lpSubKey: LPCWSTR,
        Reserved: DWORD,
        lpClass: LPCWSTR,
        dwOptions: DWORD,
        samDesired: REGSAM,
        lpSecurityAttributes: LPSECURITY_ATTRIBUTES,
        phkResult: *mut HKEY,
        lpdwDisposition: *mut DWORD,
    ) -> LSTATUS;
    
    fn RegSetValueExW(
        hKey: HKEY,
        lpValueName: LPCWSTR,
        Reserved: DWORD,
        dwType: DWORD,
        lpData: LPBYTE,
        cbData: DWORD,
    ) -> LSTATUS;
    
    fn RegCloseKey(hKey: HKEY) -> LSTATUS;

    fn MessageBoxW(hWnd: *mut c_void, lpText: LPCWSTR, lpCaption: LPCWSTR, uType: u32) -> i32;
}


fn apply_bypasses() {
    
    unsafe {

        // Wipe old compatibility markers
        RegDeleteTreeW(HKEY_LOCAL_MACHINE, to_utf16("SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\AppCompatFlags\\CompatMarkers").as_ptr());
        RegDeleteTreeW(HKEY_LOCAL_MACHINE, to_utf16("SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\AppCompatFlags\\Shared").as_ptr());
        RegDeleteTreeW(HKEY_LOCAL_MACHINE, to_utf16("SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\AppCompatFlags\\TargetVersionUpgradeExperienceIndicators").as_ptr());

        //  Inject hardware bypasses (REG_MULTI_SZ)
        let mut hkey: HKEY = std::ptr::null_mut();
        let mut disp: DWORD = 0;
        
        if RegCreateKeyExW(
            HKEY_LOCAL_MACHINE,
            to_utf16("SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\AppCompatFlags\\HwReqChk").as_ptr(),
            0,
            std::ptr::null_mut(),
            REG_OPTION_NON_VOLATILE,
            KEY_ALL_ACCESS,
            std::ptr::null_mut(),
            &mut hkey,
            &mut disp,
        ) == ERROR_SUCCESS {
            
            // Construct a double-null-terminated UTF-16 array
            let mut multi_sz: Vec<u16> = Vec::new();
            let bypasses = [
                "SQ_SecureBootCapable=TRUE", 
                "SQ_SecureBootEnabled=TRUE", 
                "SQ_TpmVersion=2", 
                "SQ_RamMB=8192"
            ];
            
            for b in bypasses {
                multi_sz.extend(b.encode_utf16());
                multi_sz.push(0); // Null separate strings
            }
            multi_sz.push(0); // Second Null terminates the array

            RegSetValueExW(
                hkey,
                to_utf16("HwReqChkVars").as_ptr(),
                0,
                REG_MULTI_SZ,
                multi_sz.as_ptr() as LPBYTE,
                (multi_sz.len() * 2) as DWORD, // Length in bytes
            );
            RegCloseKey(hkey);
        }

        // Inject mo-setup bypass (REG_DWORD) 
        if RegCreateKeyExW(
            HKEY_LOCAL_MACHINE,
            to_utf16("SYSTEM\\Setup\\MoSetup").as_ptr(),
            0,
            std::ptr::null_mut(),
            REG_OPTION_NON_VOLATILE,
            KEY_ALL_ACCESS,
            std::ptr::null_mut(),
            &mut hkey,
            &mut disp,
        ) == ERROR_SUCCESS {
            
            let val: DWORD = 1;
            RegSetValueExW(
                hkey,
                to_utf16("AllowUpgradesWithUnsupportedTPMOrCPU").as_ptr(),
                0,
                REG_DWORD,
                &val as *const DWORD as LPBYTE, 
                4, // 4 bytes for a u32/DWORD
            );
            RegCloseKey(hkey);
        }
    }
}


fn main() {
    // CD to the executable's directory
    if let Ok(mut exe_path) = env::current_exe() {
        exe_path.pop();
        let _ = env::set_current_dir(&exe_path);
    }

    // Ensure setup.dll exists so we don't bypass for nothing
    if !Path::new("setup.dll").exists() {
        unsafe {
            // 0x00000030 = MB_OK | MB_ICONWARNING
            MessageBoxW(
                std::ptr::null_mut(),
                to_utf16("This Win11 bypass requires the original setup.exe to be present as setup.dll").as_ptr(),
                to_utf16("Win11 bypass error").as_ptr(),
                0x00000030,
            );
        }
        exit(1);
    }

    // Apply Registry Hacks
    apply_bypasses();

    // Launch the real Microsoft setup.exe (setup.dll) and forward all arguments
    let args: Vec<String> = env::args().skip(1).collect();
    let mut child = Command::new("setup.dll")
        .args(args)
        .spawn()
        .expect("Failed to start setup.dll");

    let status = child.wait().expect("Failed to wait on setup.dll");
    exit(status.code().unwrap_or(1));
}


