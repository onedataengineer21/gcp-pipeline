# test_python_scripts.py
# run from top level of repo
import unittest

class TestScripts(unittest.TestCase):

    def test_first_script(self):
        self.assertEqual(5, 5)

    def test_second_script(self):
        self.assertEqual('AHOY!', 'AHOY!')

if __name__ == '__main__':
    unittest.main()
