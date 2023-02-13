#!/usr/bin/python3
"""
This script is in-charge of setting up the database in a singleton fashion,
and returning an instance on-demand.

@author: G V Datta Adithya
"""
import psycopg2 as pg
from psycopg2.extras import execute_batch
from dotenv import dotenv_values
import logging
import os
from time import perf_counter
from helpers import (
    create_table_from_col_names,
    insert_into_table_from_col_names,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


config = {**dotenv_values(".env"), **os.environ}


class DB:
    """
    An instance of a singleton connection to the Postgres Database.
    """

    _instance = None
    _tablename = "sdn"

    def __new__(cls):
        if cls._instance is None:
            logger.info("Creating the database")
            cls.conn = pg.connect(
                database=config["POSTGRES_DB"],
                user=config["POSTGRES_USER"],
                password=config["POSTGRES_PASSWORD"],
                host=config["POSTGRES_HOST"],
                port=config["POSTGRES_PORT"],
            )
            cls.cur = cls.conn.cursor()
            cls._instance = super(DB, cls).__new__(cls)
        return cls._instance

    def get_connection(self):
        """
        Returns the connection object.
        """
        return self.conn

    def get_cursor(self):
        """
        Returns the cursor object.
        """
        return self.cur

    def create_table(self, col_names):
        """
        This method is in-charge of creating a table in the database to input
        data into.
        """
        query = create_table_from_col_names(self._tablename, col_names)

        self.cur.execute(query)
        self.conn.commit()

    def batch_insert(self, col_names, data, batch_size=10000) -> None:
        """
        Batch insert records into the database.
        """
        query = insert_into_table_from_col_names(self._tablename, col_names)

        before_ds_time = perf_counter()

        for batch in range(1, len(data), batch_size):
            execute_batch(self.cur, query, data[batch : batch + batch_size])
            self.conn.commit()

            after_ds_time = perf_counter()

            logger.info(
                f"{after_ds_time - before_ds_time} | Inserted records from {batch} to {batch+batch_size}"
            )

    def truncate_table(self):
        """
        This function truncates the entire table.
        """
        query = f"TRUNCATE {self._tablename};"

        self.cur.execute(query)
        self.conn.commit()

        logger.info("The table has been truncated.")

    def get_table_continuation(self):
        """
        This function returns the number of rows in the database.
        This value is to be checked with the lines in the file, as a means
        to continue insertion from the rollback point.
        """
        query = f"SELECT count(*) FROM {self._tablename};"

        self.cur.execute(query)
        try:
            record_position = self.cur.fetchone()
            logger.info(record_position[0])

            return record_position[0]
        except Exception as err:
            logger.error(err)

