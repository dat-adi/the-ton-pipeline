#!/usr/bin/python3
"""
This is the tests file for the helper functions.

@author: G V Datta Adithya
"""
import unittest
from src.helpers import (
        create_table_from_header, 
        insert_into_table_from_header
        )


class HelperTest(unittest.TestCase):
    def test_create_table_from_header(self):
        """
        Tests the create_table_from_header() function.
        """
        query = create_table_from_header("test_table", "attr_1, attr_2, attr_3, attr_4")
        self.assertEqual(
                "CREATE TABLE IF NOT EXISTS test_table(id serial primary key, attr_1 varchar(500),  attr_2 varchar(500),  attr_3 varchar(500),  attr_4 varchar(500));",
                query,
                "The create_table_from_header function is failing."
                )


    def test_insert_into_table_from_header(self):
        """
        Tests the insert_into_table_from_header() function.
        """
        query = insert_into_table_from_header("test_table", "attr_1, attr_2, attr_3, attr_4")
        self.assertEqual(
                "INSERT INTO test_table(attr_1,  attr_2,  attr_3,  attr_4) VALUES (%s, %s, %s, %s);",
                query,
                "The create_table_from_header function is failing."
                )


if __name__ == "__main__":
    unittest.main()
