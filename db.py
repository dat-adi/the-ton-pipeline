#!/usr/bin/python3
"""
This script is in-charge of setting up the database in a singleton fashion,
and returning an instance on-demand.

@author: G V Datta Adithya
"""
import psycopg2 as pg
from dotenv import dotenv_values
import logging
import os
from time import perf_counter

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


config = {**dotenv_values(".env"), **os.environ}


class DB:
    """
    An instance of a singleton connection to the Postgres Database.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            print("Creating the database")
            cls.conn = pg.connect(
                database=config["POSTGRES_DB"],
                user=config["POSTGRES_USER"],
                password=config["POSTGRES_PASSWORD"],
                host=config["POSTGRES_HOST"],
                port=config["POSTGRES_PORT"]
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

    def create_table(self, query):
        """
        This method is in-charge of creating a table in the database to input
        data into.
        """

        self.cur.execute(query)
        self.conn.commit()

    def batch_insert(self, query, data, batch_size=10000) -> None:
        """
        Batch insert records into the database.
        """
        # There is a better way to perform this operation via mogrify, but
        # this is a testing script anyway.
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        for batch in range(0, len(data), batch_size):
            before_ds_time = perf_counter()
            self.cur.executemany(query, data[batch : batch + batch_size])
            self.conn.commit()

            after_ds_time = perf_counter()
        
            logger.info(f"{after_ds_time - before_ds_time} | Inserted records from {batch} to {batch+batch_size}")

    def truncate_table(self, query):
        """
        This function truncates the entire table.
        """
        self.cur.execute(query)
        self.conn.commit()

        logger.info("The table has been truncated.")


    def get_table_continuation(self, query):
        """
        This function returns the number of rows in the database.
        This value is to be checked with the lines in the file, as a means
        to continue insertion from the rollback point.
        """

        self.cur.execute(query)
        self.conn.commit()
        pass

