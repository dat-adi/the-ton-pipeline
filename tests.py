#!/usr/bin/python3
"""
This is the tests file for the data pipeline.

@author: G V Datta Adithya
"""
import unittest
from main import get_datasets
from db import batch_insert


class FileTest(unittest.TestCase):
    def test_get_datasets(self):
        """
        Tests the get_datasets() functionality present in main.
        """
        datasets = get_datasets()

        self.assertEqual(len(datasets), 8, "The datasets aren't matching")

class DBTest(unittest.TestCase):
    def test_batch_insert(self):
        """
        Tests whether an insert is taking place or not.
        """


if __name__ == "__main__":
    unittest.main()
        
