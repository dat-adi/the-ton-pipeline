#!/usr/bin/python3
"""
This is a script that is supposed to run a data pipeline to download and 
upload the data to a postgres database hosted on a docker container.

@author: G V Datta Adithya
"""
from db import DB
from helpers import get_datasets, get_col_names, get_records_from_dataset
import logging
from time import perf_counter


logging.basicConfig(filename="logs/logger.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main() -> None:
    """
    The main code that executes upon running the application.
    """
    init_time = perf_counter()
    col_names = get_col_names(get_datasets())

    # Connect to the database
    logger.info("Creating the database connection")

    # Creates the database
    db_conn: DB = DB()
    db_conn.create_table(col_names)

    # Getting rid of existing data through a prompt
    if input("Clear data? (y/n): ").lower()[0] == "y":
        db_conn.truncate_table()
        print("Cleared database")

    print("Writing records...")
    record_position = db_conn.get_table_continuation()

    for dataset in get_datasets():
        try:
            # Loading data from datset
            before_dataset_time = perf_counter()
            records = get_records_from_dataset(dataset)
            if len(records) > record_position:
                record_position -= len(records)
                continue

            after_dataset_time = perf_counter()

            # Update the CSV in the Database in batches of 10000 records.
            db_conn.batch_insert(col_names, records)
            logger.info(
                f"{after_dataset_time - before_dataset_time} | Inserted an entire dataset"
            )

        except FileNotFoundError as err:
            logger.error("A dataset was not found: ", err)

        except Exception as err:
            logger.error(
                f"Errors were faced while attempting to find the file: {err}"
            )

    end_time = perf_counter()
    print(end_time - init_time)

if __name__ == "__main__":
    main()
