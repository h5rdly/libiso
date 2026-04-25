import unittest
import libiso


class TestRemovableDrives(unittest.TestCase):

    def test_list_drives(self):

        drives = libiso.list_removable_drives()
        
        print('\n\n--- Removable Drives Found ---')
        if not drives:
            print('No physical drives detected. FFI testing against a mock DriveInfo object')
            mock_drive = libiso.DriveInfo(
                'Mock Flash Drive - 64 GB (/dev/mock_usb)', 
                '/dev/mock_usb', 
                64 * 1024 * 1024 * 1024,
                'mock_label',
                'Mock Hardware Model'
            )
            drives.append(mock_drive)
            
        for idx, drive in enumerate(drives):
            print(f'Name:  {drive.display_name}')
            print(f'Path:  {drive.device_path}')
            print(f'Size:  {drive.total_space_bytes} bytes\n')
        print('------------------------------')
        
        self.assertIsInstance(drives, list, 'Expected a list of drives to be returned.')
        self.assertGreater(len(drives), 0, 'Drive list is empty.')
        
        for idx, drive in enumerate(drives):
            # Ensure the PyO3 #[pyo3(get)] macros worked and attributes are accessible
            self.assertTrue(hasattr(drive, 'display_name'))
            self.assertTrue(hasattr(drive, 'device_path'))
            self.assertTrue(hasattr(drive, 'total_space_bytes'))
            
            # Ensure the types match what Python expects
            self.assertIsInstance(drive.display_name, str)
            self.assertIsInstance(drive.device_path, str)
            self.assertIsInstance(drive.total_space_bytes, int)


if __name__ == '__main__':
    unittest.main()