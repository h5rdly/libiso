import unittest

import libiso


class TestHardwareVerification(unittest.TestCase):

    def test_python_fakedrive_interaction(self):

        # 10 bytes of real RAM, but claims to be 100 bytes
        drive = libiso.FakeDrive(10, 100) 
        
        drive.write(b"ABCDEFGHIJ")
        self.assertEqual(drive.tell(), 10)
        
        # Write past the physical limit!
        drive.write(b"KLMNO")
        self.assertEqual(drive.tell(), 15)
        
        drive.seek(0, 0)
        # Because we wrote 15 bytes into 10 bytes of RAM, 
        # "KLMNO" overwrote the start! It should read: "KLMNOFGHIJ"
        data = drive.read(10)
        self.assertEqual(data, b"KLMNOFGHIJ")


    def test_sync_verification_catch_fake(self):

        # 256KB of RAM, claiming to be 1MB
        drive = libiso.FakeDrive(256 * 1024, 1024 * 1024)
        
        with self.assertRaises(RuntimeError) as context:
            libiso.test_verify_fake_drive_sync(drive)
            
        self.assertIn("Fake drive detected", str(context.exception))


    def test_sync_verification_honest_drive(self):

        drive = libiso.FakeDrive(1024 * 1024, 1024 * 1024)
        
        try:
            libiso.test_verify_fake_drive_sync(drive)
        except Exception as e:
            self.fail(f"Verification failed on an honest drive: {e}")


if __name__ == '__main__':
    unittest.main()