# test_python_scripts.py
# run from top level of repo
import unittest

class TestScripts(unittest.TestCase):

    def test_extract_userdata(self):
        self.assertEqual(5, 5)

if __name__ == '__main__':
    unittest.main()
