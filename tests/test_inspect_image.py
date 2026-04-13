import os
import tempfile
import unittest
import libiso

class TestInspectImage(unittest.TestCase):

    def _generate_and_test(self, volume_name: str, files: list[str], is_isohybrid: bool):
        '''Helper to forge an ISO in RAM, write to temp file, and parse it.'''
        iso_bytes = libiso.create_mock_iso(volume_name, files, is_isohybrid)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.iso') as tmp:
            tmp.write(iso_bytes)
            tmp_path = tmp.name
            
        try:
            report = libiso.inspect_image(tmp_path)
            return report
        finally:
            os.unlink(tmp_path)


    def test_linux_isohybrid_detection(self):

        # Standard Linux ISO (ISOHybrid + UEFI bootloader)
        report = self._generate_and_test(
            volume_name='UBUNTU_LIVE',
            files=['BOOTX64.EFI'], 
            is_isohybrid=True
        )
        
        self.assertEqual(report.volume_label, 'UBUNTU_LIVE')
        self.assertTrue(report.is_isohybrid)
        self.assertTrue(report.boot_info.supports_uefi)
        self.assertTrue(report.boot_info.supports_bios) # BIOS is supported via ISOHybrid
        self.assertTrue(report.boot_info.is_bootable)
        self.assertIsNone(report.windows_info)


    def test_windows_10_detection(self):

        # Windows 10 ISO (Standard UEFI, WIM file, NO Appraiserres)
        report = self._generate_and_test(
            volume_name='CCCOMA_X64',
            files=['BOOTX64.EFI', 'INSTALL.WIM'], 
            is_isohybrid=False
        )
        
        self.assertFalse(report.is_isohybrid)
        self.assertTrue(report.boot_info.supports_uefi)
        
        # Windows Metadata Checks
        self.assertIsNotNone(report.windows_info)
        self.assertTrue(report.windows_info.is_windows)
        self.assertFalse(report.windows_info.is_windows_11)
        self.assertEqual(report.windows_info.install_image_type, 'wim')
        self.assertTrue(report.windows_info.supports_wintogo)


    def test_windows_11_detection(self):

        # Windows 11 ISO (ESD file + Win11 Appraiserres DLL)
        report = self._generate_and_test(
            volume_name='WIN11_MOCK',
            files=['BOOTX64.EFI', 'INSTALL.ESD', 'APPRAISERRES.DLL'], 
            is_isohybrid=False
        )
        
        self.assertTrue(report.windows_info.is_windows)
        self.assertTrue(report.windows_info.is_windows_11)
        self.assertEqual(report.windows_info.install_image_type, 'esd')


    def test_missing_file(self):
        
        with self.assertRaises(FileNotFoundError):
            libiso.inspect_image('/path/to/literally/nowhere.iso')


if __name__ == '__main__':
    unittest.main()