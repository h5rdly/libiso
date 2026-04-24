import os, sys, time, tempfile

import unittest
from unittest.mock import patch

import libiso


if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


class TestWriterIso(unittest.TestCase):

    def setUp(self):

        # Create a 64MB dummy file to act as our 'USB Drive'
        self.dest_fd = tempfile.NamedTemporaryFile(delete=False)
        self.dest_fd.write(b'\x00' * (64 * 1024 * 1024))
        self.dest_fd.close()
        self.dest_path = self.dest_fd.name

        # Create a real small ISO for extraction testing
        self.iso_content = libiso.create_mock_iso('TEST_ISO', ['EFI/BOOT/BOOTX64.EFI', 'KERNEL.BIN'], True, 10)
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


    def test_dd_write_large_file(self):

        print('\n--- Testing DD Raw Write (50MB) ---')
        
        stream = libiso.write_image_dd(
            self.large_source_path,
            self.dest_path
        )

        for event in stream:
            if event.msg_type == 'PROGRESS':
                # Sleep just enough to watch the 4MB chunks fly by
                time.sleep(0.1)
                ratio = event.written / event.total
                percent = (ratio) * 100 if event.total > 0 else 0
                bar = '█' * int(40 * ratio)
                bar = bar.ljust(40, '-')
                print(f'\r\033[KDD Mode Write:  |{bar}| {percent:.1f}% ({event.written}/{event.total} bytes)', end='')
            
        print('\nDD Write Complete!')


    @patch('libiso.ensure_uefi_bridge')
    def test_burn_image_iso_mode(self, mock_uefi_bridge):

        print('\n--- Testing High-Level burn_image (ISO Mode) ---')
        
        # Mock the UEFI bridge download 
        uefi_fd = tempfile.NamedTemporaryFile(delete=False)
        uefi_fd.write(b'\x00' * (1024 * 1024))
        uefi_fd.close()
        mock_uefi_bridge.return_value = uefi_fd.name

        try:
            # omitting partition_scheme to force auto decide
            libiso.burn_image(self.source_path, self.dest_path, method='ISO')

            # Verify the Rust backend touched the destination disk
            with open(self.dest_path, 'rb') as f:
                first_chunk = f.read(512)
                # The MBR/Protective MBR signature 0x55AA should be at the end of sector 0
                self.assertEqual(first_chunk[510:], b'\x55\xAA')

        finally:
            if os.path.exists(uefi_fd.name):
                os.remove(uefi_fd.name)


    def test_burn_image_dd_mode(self):

        print('\n--- Testing High-Level burn_image (DD Mode) ---')  
        libiso.burn_image(self.large_source_path, self.dest_path, method='DD')
        
        expected_size = 50 * 1024 * 1024 # 50 MB
        self.assertEqual(os.path.getsize(self.large_source_path), expected_size)
        
        with open(self.dest_path, 'rb') as dest_f, open(self.large_source_path, 'rb') as src_f:
            dest_data = dest_f.read(expected_size)
            src_data = src_f.read(expected_size)
            
            self.assertEqual(dest_data, src_data)
            self.assertEqual(len(dest_data), expected_size)


    def test_iso_extraction_full_cycle(self):

        print('\n--- Testing ISO Extraction (exFAT) ---')
        
        # dummy 1MB UEFI bridge image for the test
        uefi_fd = tempfile.NamedTemporaryFile(delete=False)
        uefi_fd.write(b'\x00' * (1024 * 1024)) # 1MB of zeros
        uefi_fd.close()
        uefi_path = uefi_fd.name

        stream = libiso.write_image_iso(
            self.source_path, 
            self.dest_path, 
            True,  # has_large_file - Force exFAT
            'TEST LABEL',
            'GPT',
            uefi_ntfs_path=uefi_path
        )

        for event in stream:

            if event.msg_type != 'PROGRESS':
                continue

            # slow down the loop so the human eye can see it
            # This triggers backpressure on the Rust thread
            time.sleep(10** -4) 
            ratio = event.written / event.total
            ratio = event.written / event.total
            percent = (ratio) * 100 if event.total > 0 else 0
            bar = '█' * int(40 * ratio)
            bar = bar.ljust(40, '-')
            print(f'\r\033[KISO Extraction: |{bar}| {percent:.1f}%', end='')
        
        print('\nExtraction Complete!')

        if os.path.exists(uefi_path):
            os.remove(uefi_path)

        with open(self.dest_path, 'rb') as f:
            first_chunk = f.read(512)
            # MBR signature 0x55AA should be at the end of sector 0
            self.assertEqual(first_chunk[510:], b'\x55\xAA')


    def test_strict_uefi_compliance_and_ascii_map(self):
        
        print('\n--- Testing Strict UEFI/GPT Compliance & Generating ASCII Map ---')
        
        # Create a dummy UEFI bridge image for the test
        uefi_fd = tempfile.NamedTemporaryFile(delete=False)
        uefi_fd.write(b'\x00' * (1024 * 1024))
        uefi_fd.close()
        uefi_path = uefi_fd.name

        try:
            # 1. Run the burn process to format the FakeDrive
            stream = libiso.write_image_iso(
                self.source_path, 
                self.dest_path, 
                True,          # has_large_file
                'LIBISO_USB',  # usb_label
                'GPT',         # partition_scheme
                uefi_ntfs_path=uefi_path
            )
            for _ in stream: pass

            # 2. Cryptographically Verify the FakeDrive bytes!
            import struct, zlib

            with open(self.dest_path, 'rb') as f:
                f.seek(0, os.SEEK_END)
                total_sectors = f.tell() // 512

                # --- A. Check Protective MBR (LBA 0) ---
                f.seek(0)
                mbr = f.read(512)
                self.assertEqual(mbr[510:512], b'\x55\xaa', 'MBR Signature missing')
                
                status, start_chs, ptype, end_chs, start_lba, sectors = struct.unpack('<B3sB3sII', mbr[446:462])
                
                self.assertEqual(ptype, 0xEE, 'Protective MBR must have type 0xEE')
                self.assertEqual(start_lba, 1, 'Protective MBR must cover from LBA 1')
                self.assertNotEqual(start_chs, b'\xFE\xFF\xFF', 'FATAL: CHS Start is invalidly maxed out! This triggers Windows disk corruption.')

                # --- B. Check Primary GPT Header (LBA 1) ---
                f.seek(512)
                hdr = f.read(512)
                sig, rev, hdr_size, crc, _, cur_lba, bak_lba, first_usable, last_usable, disk_guid, part_start, num_parts, part_size, part_crc = struct.unpack('<8sIIIIQQQQ16sQIII', hdr[:92])
                
                self.assertEqual(sig, b'EFI PART', 'Primary GPT Signature Missing')
                self.assertEqual(first_usable, 34, 'First usable LBA must be 34 (after header + array)')
                self.assertEqual(last_usable, total_sectors - 34, 'Last usable LBA must protect the final 33 sectors!')
                self.assertNotEqual(disk_guid, b'\x00'*16, 'Disk GUID must not be all zeros!')
                
                # Verify Header CRC
                zeroed_hdr = hdr[:16] + b'\x00\x00\x00\x00' + hdr[20:hdr_size]
                actual_crc = zlib.crc32(zeroed_hdr) & 0xFFFFFFFF
                self.assertNotEqual(crc, 0, 'Primary Header CRC is 0x0 (Uncalculated!)')
                self.assertEqual(crc, actual_crc, 'Primary Header CRC Mismatch!')

                # --- C. Check Partition 1 (LBA 2) ---
                f.seek(part_start * 512)
                part1 = f.read(128)
                type_guid, uniq_guid, p_start, p_end, attrs, name_utf16 = struct.unpack('<16s16sQQQ72s', part1)
                
                self.assertEqual(p_start, 2048, 'Partition 1 must be exactly 1 MiB (2048 LBA) aligned!')
                self.assertNotEqual(uniq_guid, b'\x00'*16, 'Partition GUID must not be all zeros!')

                # --- D. Check Backup GPT Header (Last LBA) ---
                f.seek((total_sectors - 1) * 512)
                bak_hdr = f.read(512)
                self.assertEqual(bak_hdr[:8], b'EFI PART', 'Backup GPT Header missing from the absolute end of the disk!')

            # 3. If all assertions pass, print the beautiful ASCII map using the library function
            print(libiso.generate_disk_layout_ascii(self.dest_path))
                
        finally:
            if os.path.exists(uefi_path):
                os.remove(uefi_path)


if __name__ == '__main__':
    unittest.main()