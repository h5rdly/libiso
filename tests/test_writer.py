import tempfile
import sys
import libiso


def progress_callback(written: int, total: int):
    # Calculate percentage
    percent = (written / total) * 100
    
    # Print a beautiful progress bar
    bar_length = 40
    filled = int(bar_length * (written / total))
    bar = '=' * filled + '-' * (bar_length - filled)
    
    # \r overwrites the current line in the terminal!
    sys.stdout.write(f"\rWriting: [{bar}] {percent:.1f}% ({written}/{total} bytes)")
    sys.stdout.flush()

# 1. Ask Rust to forge a 2MB mock ISO in RAM
iso_bytes = libiso.create_mock_iso("TEST_ISO", ["BOOTX64.EFI"], True)

# 2. Write it to a temporary "Source" file
with tempfile.NamedTemporaryFile(delete=False) as source_file:
    source_file.write(iso_bytes)
    source_path = source_file.name

# 3. Create a temporary "Destination" file (pretend this is /dev/sdb)
with tempfile.NamedTemporaryFile(delete=False) as dest_file:
    dest_path = dest_file.name

print(f"Starting DD Mode Write...")
print(f"Source: {source_path}")
print(f"Dest:   {dest_path}\n")

# 4. FIRE THE RUST WRITER!
try:
    libiso.write_image_dd(source_path, dest_path, progress_callback)
    print("\n\nWrite Completed Successfully!")
except Exception as e:
    print(f"\n\nWrite Failed: {e}")

def mock_callback(written: int, total: int):
    pass # We aren't extracting files yet!

# Create a 64MB dummy file to act as our "USB Drive"
with tempfile.NamedTemporaryFile(delete=False) as dest_file:
    dest_file.write(b'\x00' * (64 * 1024 * 1024)) 
    dest_path = dest_file.name

print(f"Testing Phase 1: GPT Partitioning on {dest_path}")

try:
    # Notice we pass `has_large_file=False` for now
    libiso.write_image_iso("Cargo.toml", dest_path, False, mock_callback)
    print("Success! The mock drive now has a valid UEFI GPT partition table.")
except Exception as e:
    print(f"Failed: {e}")