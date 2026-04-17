import os, tempfile

import unittest

import libiso


class TestSignatureVerification(unittest.TestCase):

    def _generate_mock_iso_and_test(self, files: list[str]):

        iso_bytes = libiso.create_mock_iso('SECURE_MOCK', files, False, 10)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.iso') as tmp:
            tmp.write(iso_bytes)
            tmp_path = tmp.name
            
        try:
            report = libiso.inspect_image(tmp_path)
            return report
        finally:
            os.unlink(tmp_path)


    def test_corrupted_or_unsigned_pe(self):

        # The mock generator creates a dummy file, not a real PE executable.
        # The Rust backend should find it and flag it as unsigned.
        
        report = self._generate_mock_iso_and_test(['BOOTX64.EFI'])
        
        self.assertTrue(report.boot_info.supports_uefi)
        self.assertFalse(report.boot_info.secure_boot_signed)
        self.assertFalse(report.boot_info.is_microsoft_signed)
        self.assertFalse(report.boot_info.is_revoked)
        self.assertEqual(report.boot_info.signature_size, 0)


    def test_iso_with_missing_bootloader(self):

        report = self._generate_mock_iso_and_test(['RANDOM_DATA.TXT'])
        
        self.assertFalse(report.boot_info.supports_uefi)
        self.assertFalse(report.boot_info.secure_boot_signed)
        self.assertFalse(report.boot_info.is_microsoft_signed)
        self.assertFalse(report.boot_info.is_revoked)


if __name__ == '__main__':
    unittest.main()