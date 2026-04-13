import os
import tempfile
import unittest
import libiso


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


    def tearDown(self):

        if os.path.exists(self.dest_path):
            os.remove(self.dest_path)
        if os.path.exists(self.source_path):
            os.remove(self.source_path)


    def test_iso_extraction_full_cycle(self):
        
        def progress(written, total):
            pass  # Silent for tests

        # This should perform Partitioning -> Formatting -> Extraction
        try:
            libiso.write_image_iso(self.source_path, self.dest_path, False, progress)
        except Exception as e:
            self.fail(f"write_image_iso raised exception: {e}")

        # Basic verification: Check if the file is no longer just zeros
        with open(self.dest_path, "rb") as f:
            first_chunk = f.read(512)
            # MBR signature 0x55AA should be at the end
            self.assertEqual(first_chunk[510:], b"\x55\xAA")
            
            # Check partition start (1MB = 1048576)
            f.seek(1048576)
            partition_start = f.read(3)
            # FAT32 starts with jump instruction 0xEB or 0xE9
            self.assertIn(partition_start[0], [0xEB, 0xE9])


if __name__ == "__main__":
    unittest.main()