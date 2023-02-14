#!/usr/bin/python3
"""
This is the file that holds helper functions for the database operations in 
Python.

@author: G V Datta Adithya
"""
import os
from pathlib import Path
import logging
import csv

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(BASE_DIR, "data/network_dataset")


def get_datasets() -> list:
    """
    Returns the datasets present in the data directory.
    """
    try:
        datasets = [
            os.path.join(DATA_DIR, x) for x in sorted(os.listdir(DATA_DIR))
        ]

        return datasets
    except FileNotFoundError:
        logger.error(
            "The network dataset folder is missing or the path hasn't been set properly."
        )
        if (
            input("Set your path correctly and try again? (y/n): ").lower()[0]
            == "y"
        ):
            get_datasets()
        exit(0)


def get_col_names(datasets):
    """
    Return the column names.
    """
    try:
        with open(datasets[0], "r") as csvfile:
            csv_recs = csv.reader(csvfile)
            for row in csv_recs:
                return row
    except FileNotFoundError:
        logger.error("The CSV file was not found")
    except Exception as err:
        logger.error(err)


def get_records_from_dataset(dataset):
    """
    Returns the records from the dataset.
    """
    records: list = []
    with open(dataset, "r") as csvfile:
        csv_recs = csv.reader(csvfile)
        for row in csv_recs:
            records.append(tuple(row))

        csvfile.close()

    return records


def create_table_from_col_names(table_name, col_names):
    """
    This function reads from the header and produces a CREATE_TABLE_QUERY.
    """

    query = (
        f"""CREATE TABLE IF NOT EXISTS {table_name}(id serial primary key, """
    )

    for attribute in col_names[:-1]:
        query += attribute + " varchar(500), "

    query += col_names[-1] + " varchar(500)"

    query += ");"

    return query


def insert_into_table_from_col_names(table_name, col_names):
    """
    This function reads from the header and produces a INSERT_TABLE_QUERY.
    """

    query = f"INSERT INTO {table_name}("
    parameters = "("

    for attribute in col_names[:-1]:
        query += attribute + ", "
        parameters += r"%s, "

    query += col_names[-1]
    query += ")"

    parameters += "%s)"
    query += f" VALUES "

    return query, parameters


if __name__ == "__main__":
    print(
        insert_into_table_from_col_names(
            "test_table", ["attr_1", "attr_2", "attr_3", "attr_4"]
        )
    )
