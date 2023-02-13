#!/usr/bin/python3
"""
This is the tests file for the helper functions.

@author: G V Datta Adithya
"""
import unittest
from src.helpers import (
        get_datasets,
        get_col_names,
        get_records_from_dataset,
        create_table_from_col_names,
        insert_into_table_from_col_names
        )
from pathlib import Path


class HelperTest(unittest.TestCase):
    def test_get_datasets(self):
        """
        Tests the get_datasets() function.
        """
        self.assertIsInstance(get_datasets(), list, "The datatype of the instance is incorrect.")

    def test_get_col_names(self):
        """
        Tests the get_col_names() function.
        """
        self.assertIsInstance(get_col_names(), list, "The datatype is incorrect.")

    def test_get_records_from_dataset(self):
        """
        Returns records from the dataset.
        """
        pass

    def test_create_table_from_col_names(self):
        """
        Tests the CREATE TABLE QUERY function.
        """
        query = create_table_from_col_names("test_table", ["attr_1", "attr_2", "attr_3", "attr_4"])
        self.assertEqual(
                "CREATE TABLE IF NOT EXISTS test_table(id serial primary key, attr_1 varchar(500), attr_2 varchar(500), attr_3 varchar(500), attr_4 varchar(500));",
                query,
                "The create_table_from_header function is failing."
                )


    def test_insert_into_table_from_header(self):
        """
        Tests the INSERT INTO TABLE QUERY function.
        """
        query = insert_into_table_from_col_names("test_table", ["attr_1", "attr_2", "attr_3", "attr_4"])
        self.assertEqual(
                "INSERT INTO test_table(attr_1, attr_2, attr_3, attr_4) VALUES (%s, %s, %s, %s);",
                query,
                "The create_table_from_header function is failing."
                )


if __name__ == "__main__":
    unittest.main()
