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

logging.basicConfig(filename="logs/logger.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_datasets() -> list:
    """
    Returns the datasets present in the data directory.
    """
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = os.path.join(BASE_DIR, "data/network_dataset")
    datasets = [os.path.join(DATA_DIR, x) for x in sorted(os.listdir(DATA_DIR))]

    return datasets


def main() -> None:
    """
    The main code that executes upon running the application.
    """
    records: List[tuple] = []
    datasets: list = get_datasets()

    # Connect to the database
    logger.info("Creating the database connection")

    db_conn: DB = DB()
    db_conn.create_table()

    for dataset in datasets:
        try:
            # Loading data from datset
            with open(dataset, "r") as csvfile:
                csv_recs = csv.reader(csvfile)
                for row in csv_recs:
                    records.append(tuple(row))

                csvfile.close()

            # Update the CSV in the Database in batches of 10000 records.
            db_conn.batch_insert(records)
            logger.info("Inserted an entire dataset")
        except FileNotFoundError as err:
            logger.error("A dataset was not found: ", err)
        except Exception as err:
            logger.error(f"Errors were faced while attempting to find the file: {err}")



if __name__ == "__main__":
    main()
