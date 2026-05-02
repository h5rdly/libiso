import os

import unittest

import libiso


class TestInitramfsPatcher(unittest.TestCase):
    
    def setUp(self):

        # Path to the actual OpenMandriva initrd you extracted
        self.initrd_path = "OPENMANDRIV/boot/liveinitrd.img"
        self.output_path = "patched_liveinitrd.img"
        
        # A dummy "kernel module" payload to inject
        self.dummy_payload = b"LIBISO_TEST_PAYLOAD_12345"
        self.target_inject_path = "usr/lib/modules/libiso_test.ko"


    def tearDown(self):
        # Optional: clean up the patched file after the test
        # if os.path.exists(self.output_path):
        #     os.remove(self.output_path)
        pass


    def test_compression_sandwich(self):

        if not os.path.exists(self.initrd_path):
            self.skipTest(f"Could not find {self.initrd_path}. Please extract it from the ISO first.")

        print(f"\n[*] Reading original initramfs: {self.initrd_path}")
        with open(self.initrd_path, "rb") as f:
            raw_initramfs = f.read()

        original_size = len(raw_initramfs)
        print(f"[*] Original Size: {original_size / (1024*1024):.2f} MB")

        # 1. Call the Rust backend to Decompress -> Inject -> Recompress
        print(f"[*] Calling libiso Rust backend to inject {self.target_inject_path}...")
        try:
            patched_initramfs = libiso.patch_initramfs_py(
                raw_initramfs, 
                self.dummy_payload, 
                self.target_inject_path
            )
        except Exception as e:
            self.fail(f"Rust backend threw an exception: {e}")

        patched_size = len(patched_initramfs)
        print(f"[*] Patched Size: {patched_size / (1024*1024):.2f} MB")

        # 2. Write it to disk so you can inspect it manually or boot it!
        with open(self.output_path, "wb") as f:
            f.write(patched_initramfs)
            
        print(f"[*] Successfully saved to {self.output_path}")

        # 3. Basic sanity assertions
        self.assertTrue(patched_size > 0, "Patched initramfs is empty!")
        
        # The patched archive should be slightly larger (or very close in size) due to the new file
        # Compression algorithms differ slightly, so we just ensure it didn't completely corrupt to 0
        size_diff = abs(original_size - patched_size)
        self.assertTrue(size_diff < (original_size * 0.10), "Patched archive size changed drastically, compression may have failed!")



if __name__ == '__main__':
    unittest.main()