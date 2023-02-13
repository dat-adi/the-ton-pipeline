#!/usr/bin/python3
"""
This is a script that is supposed to run a data pipeline to download and 
upload the data to a postgres database hosted on a docker container.

@author: G V Datta Adithya
"""
import csv
from db import DB
from typing import List
from pathlib import Path
import os
import logging
from time import perf_counter

logging.basicConfig(filename="logs/logger.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(BASE_DIR, "data/network_dataset")

def get_datasets() -> list:
    """
    Returns the datasets present in the data directory.
    """
    try:
        datasets = [os.path.join(DATA_DIR, x) for x in sorted(os.listdir(DATA_DIR))]

        return datasets
    except FileNotFoundError:
        logger.error("The network dataset folder is missing or the path hasn't been set properly.")
        if input("Set your path correctly and try again? (y/n): ").lower()[0] == "y":
            get_datasets()
        exit(0)

def get_col_names():
    """
    Return the column names.
    """
    try:
        with open(get_datasets()[0], "r") as csvfile:
            csv_recs = csv.reader(csvfile)
            for row in csv_recs:
                return row
    except FileNotFoundError:
        logger.error("The CSV file was not found")
    except Exception as err:
        logger.error(err)

def main() -> None:
    """
    The main code that executes upon running the application.
    """
    records: List[tuple] = []
    datasets: list = get_datasets()
    col_names = get_col_names()

    # Connect to the database
    logger.info("Creating the database connection")

    db_conn: DB = DB()
    db_conn.create_table(col_names)

    # Getting rid of existing data through a prompt
    if input("Clear data? (y/n): ").lower()[0] == "y":
        db_conn.truncate_table()

    for dataset in datasets:
        try:
            # Loading data from datset
            before_dataset_time = perf_counter()

            with open(dataset, "r") as csvfile:
                csv_recs = csv.reader(csvfile)
                for row in csv_recs:
                    records.append(tuple(row))

                csvfile.close()

            after_dataset_time = perf_counter()
            # Update the CSV in the Database in batches of 10000 records.
            db_conn.batch_insert(col_names, records)
            logger.info(f"{after_dataset_time - before_dataset_time} | Inserted an entire dataset")
        except FileNotFoundError as err:
            logger.error("A dataset was not found: ", err)
        except Exception as err:
            logger.error(f"Errors were faced while attempting to find the file: {err}")



if __name__ == "__main__":
    main()
