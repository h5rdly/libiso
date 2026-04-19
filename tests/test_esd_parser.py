import os, time, tempfile

import unittest

import libiso 



class TestEsdParser(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        '''Generate a mock ESD file in memory and write to a tempfile.'''

        print(f'\n--- Generating Mock ESD File ---')
        esd_bytes = libiso.create_mock_esd()
        
        cls.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.esd')
        cls.temp_file.write(esd_bytes)
        cls.temp_file.close()

        cls.esd = libiso.EsdArchive(cls.temp_file.name)

    @classmethod
    def tearDownClass(cls):
        '''Cleanup file handles and tempfile.'''

        del cls.esd
        os.remove(cls.temp_file.name)


    def test_header_parsing(self):
        ''' Verify that Rust successfully parsed the header and mapped the chunks '''

        self.assertGreater(self.esd.num_chunks, 0, 'Solid resource must contain at least 1 chunk.')
        self.assertEqual(self.esd.num_chunks, 2, 'Mock ESD should have exactly 2 chunks.')
        print(f'Total Chunks Mapped: {self.esd.num_chunks}')


    def test_xml_extraction(self):
        ''' Verify that the XML metadata is natively extracted during ISO inspection '''

        print('\n--- Inspecting WIM/ESD Metadata ---')
        
        stats = libiso.inspect_image(self.temp_file.name)
        
        self.assertIsNotNone(stats.windows_info, 'Windows metadata was not extracted.')
        
        win_info = stats.windows_info
        self.assertIn('Pro', win_info.editions, 'Did not find Pro edition in mock XML.')
        self.assertEqual(win_info.architecture, 'x64')
        
        print(f'Detected Architecture: {win_info.architecture}')
        print(f'Detected Editions: {", ".join(win_info.editions)}')


    def test_file_tree_extraction(self):
        ''' Verify the Rust pipeline safely catches bad LZMS data without crashing '''

        print('\n--- Handing off Chunk 0 File Tree Parsing to Rust Engine ---')
        
        start_time = time.time()
        
        # Since the mock data is literally just 0x42 0x42 0x42, it WILL fail decompression.
        # We test that it successfully reaches the Rust engine and throws a standard 
        # Python Exception rather than causing a catastrophic Rust panic.
        with self.assertRaises(Exception) as context:
            self.esd.get_wim_file_tree(0)
            
        elapsed = time.time() - start_time
        
        print(f"✅ Rust engine successfully intercepted invalid LZMS block: '{context.exception}'")
        print(f"Safety check completed in {elapsed:.4f} seconds.")


if __name__ == '__main__':
    unittest.main(verbosity=2)