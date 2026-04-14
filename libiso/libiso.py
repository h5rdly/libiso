import os, sys, importlib.util
from typing import Optional, Callable


UEFI_BRIDGE_NAME = 'uefi-ntfs.img'
UEFI_BRIDGE_DOWNLOAD_URL = f'https://raw.githubusercontent.com/pbatard/rufus/master/res/uefi/{UEFI_BRIDGE_NAME}'


## -- Load Rust module

def _load_module(module_name: str, path: str):

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    return None


def _load_rust_pip_or_dev(_rust_lib_name: str = '_libiso', module_dev_path: str = None):
    
    # Wheel load (pip install)
    try:
        from . import _libiso
        return _libiso
    except ImportError:
        pass
    
    if os.name == 'nt':
        expected_file = f'{_rust_lib_name}.dll' 
    elif sys.platform == 'darwin':
        expected_file = f'lib{_rust_lib_name}.dylib'
    else:
        expected_file = f'lib{_rust_lib_name}.so'

    py_dir = __file__.replace('\\', '/').rsplit('/', 1)[0]
    root_dir = py_dir.rsplit('/', 1)[0]
    dev_path = module_dev_path or f'{root_dir}/target/release/{expected_file}'
    
    # Search local directory (for maturin develop)
    for file in os.listdir(py_dir):
        if ((file.startswith(f'lib{_rust_lib_name}') or file.startswith(_rust_lib_name)) 
                and file.endswith(('.so', '.pyd', '.dylib', '.dll'))):
            mod = _load_module(_rust_lib_name, f'{py_dir}/{file}')
            if mod: return mod
    
    # Maturin build in CI
    if os.path.exists(dev_path):
        mod = _load_module(_rust_lib_name, dev_path)
        if mod: return mod

    raise ImportError(f'Could not find Rust binary. Tried local dir and {dev_path}')

_rust_lib = _load_rust_pip_or_dev()
globals().update({k: v for k, v in vars(_rust_lib).items() if not k.startswith('__')})



## -- General helper functions

def ensure_uefi_bridge(cache_dir='.'):
    ''' Downloads the UEFI bridge image if it doesn't already exist '''

    local_path = os.path.join(cache_dir, UEFI_BRIDGE_NAME)
    
    if not os.path.exists(local_path):
        print(f'Downloading UEFI:NTFS bridge to {local_path}...')

        import urllib.request, urllib.error
        try:
            urllib.request.urlretrieve(UEFI_BRIDGE_DOWNLOAD_URL, local_path)
        except urllib.error.URLError as e:
            raise RuntimeError(f'Failed to download UEFI bridge: {e}')
            
    return local_path


def _progress_bar(written: int, total: int):
    
    if total == 0:
        return
        
    percent = (written / total) * 100
    bar_len = 40
    filled = int(bar_len * written / total)
    bar = '█' * filled + '-' * (bar_len - filled)
    
    # \r goes to start of line, \033[K clears the rest of the line
    sys.stdout.write(f'\r\033[KBurning: |{bar}| {percent:.1f}% ({written}/{total} bytes)')
    sys.stdout.flush()
    if written >= total:
        sys.stdout.write('\n')


def burn_image(
    image_path: str, 
    device_path: str, 
    method: str = 'iso', 
    show_progress: bool = True,
    progress_callback: Optional[Callable[[int, int], None]] = None
):
    '''
    High-level entry point for burning an image to a drive.
    
    :param image_path: Path to the .iso file.
    :param device_path: Target device path (e.g., '/dev/sdb' or '\\\\.\\PhysicalDrive1').
    :param method: 'iso' for filesystem extraction, 'dd' for raw block writing.
    :param show_progress: If True and no callback is provided, prints a terminal bar.
    :param progress_callback: Custom callback function(written_bytes, total_bytes).
    '''
    if not os.path.exists(image_path):
        raise FileNotFoundError(f'Image not found: {image_path}')

    # Determine the callback to pass to Rust
    cb = progress_callback
    if cb is None and show_progress:
        cb = _progress_bar
    elif cb is None:
        cb = lambda w, t: None  # Silent

    method = method.lower()

    if method == 'dd':
        print(f'Starting raw DD write from {image_path} to {device_path}...')
        libiso.write_image_dd(image_path, device_path, cb)
        print('DD Write complete!')
        return

    if method == 'iso':
        print(f'Inspecting ISO: {image_path}')
        stats = libiso.inspect_image(image_path)
        
        # Access the dictionary fields based on what your inspect_image returns
        has_large_file = stats.get('Large File (>4GB)', False)
        
        uefi_path = None
        if has_large_file:
            print('Large file (>4GB) detected! Ensuring UEFI:NTFS bridge is present...')
            uefi_path = ensure_uefi_bridge()

        print(f'Starting ISO filesystem extraction to {device_path}...')
        libiso.write_image_iso(
            image_path=image_path,
            device_path=device_path,
            has_large_file=has_large_file,
            callback=cb,
            uefi_ntfs_path=uefi_path
        )
        print('ISO Extraction complete!')
        return

    raise ValueError(f"Unknown method: '{method}'. Use 'iso' or 'dd'")




