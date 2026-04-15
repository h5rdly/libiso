import os, sys, time, tempfile

import unittest

import libiso


if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


class TestWriterIso(unittest.TestCase):

    def setUp(self):

        # Create a 64MB dummy file to act as our "USB Drive"
        self.dest_fd = tempfile.NamedTemporaryFile(delete=False)
        self.dest_fd.write(b'\x00' * (64 * 1024 * 1024))
        self.dest_fd.close()
        self.dest_path = self.dest_fd.name

        # Create a real small ISO for extraction testing
        self.iso_content = libiso.create_mock_iso("TEST_ISO", ["EFI/BOOT/BOOTX64.EFI", "KERNEL.BIN"], True)
        self.source_fd = tempfile.NamedTemporaryFile(delete=False)
        self.source_fd.write(self.iso_content)
        self.source_fd.close()
        self.source_path = self.source_fd.name

        # Create a 50MB dummy file for DD streaming test
        self.large_source_fd = tempfile.NamedTemporaryFile(delete=False)
        self.large_source_fd.write(os.urandom(50 * 1024 * 1024))
        self.large_source_fd.close()
        self.large_source_path = self.large_source_fd.name


    def tearDown(self):
        for path in [self.dest_path, self.source_path, self.large_source_path]:
            if os.path.exists(path):
                os.remove(path)


    def test_iso_extraction_full_cycle(self):

        print("\n--- Testing ISO Extraction (exFAT) ---")
        
        # dummy 1MB UEFI bridge image for the test
        uefi_fd = tempfile.NamedTemporaryFile(delete=False)
        uefi_fd.write(b'\x00' * (1024 * 1024)) # 1MB of zeros
        uefi_fd.close()
        uefi_path = uefi_fd.name

        stream = libiso.write_image_iso(
            self.source_path, 
            self.dest_path, 
            True,  # has_large_file - Force exFAT
            'GPT',
            uefi_ntfs_path=uefi_path
        )

        for written, total in stream:
            # slow down the loop so the human eye can see it.
            # This triggers backpressure on the Rust thread
            time.sleep(0.05) 
            
            percent = (written / total) * 100 if total > 0 else 0
            bar = '█' * int(40 * written / total)
            bar = bar.ljust(40, '-')
            print(f"\r\033[KISO Extraction: |{bar}| {percent:.1f}%", end="")
        
        print("\nExtraction Complete!")

        if os.path.exists(uefi_path):
            os.remove(uefi_path)

        with open(self.dest_path, "rb") as f:
            first_chunk = f.read(512)
            # MBR signature 0x55AA should be at the end of sector 0
            self.assertEqual(first_chunk[510:], b"\x55\xAA")


    def test_dd_write_large_file(self):

        print("\n--- Testing DD Raw Write (50MB) ---")
        
        stream = libiso.write_image_dd(
            self.large_source_path,
            self.dest_path
        )

        for written, total in stream:
            # Sleep just enough to watch the 4MB chunks fly by
            time.sleep(0.1)
            
            percent = (written / total) * 100 if total > 0 else 0
            bar = '█' * int(40 * written / total)
            bar = bar.ljust(40, '-')
            print(f"\r\033[KDD Mode Write:  |{bar}| {percent:.1f}% ({written}/{total} bytes)", end="")
        
        print("\nDD Write Complete!")


if __name__ == "__main__":
    unittest.main()