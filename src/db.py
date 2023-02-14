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

    def get_tablename(self):
        """
        Returns the tablename for the object.
        """
        return self._tablename

class DBOperations:
    def __init__(self, cur, conn, tablename):
        self.cur = cur
        self.conn = conn
        self._tablename = tablename

    def create_table(self, col_names):
        """
        This method is in-charge of creating a table in the database to input
        data into.
        """
        query = create_table_from_col_names(self._tablename, col_names)

        self.cur.execute(query)
        self.conn.commit()

    def batch_insert(self, col_names, data, record_position=1, batch_size=10000) -> None:
        """
        Batch insert records into the database.
        """
        query, parameters = insert_into_table_from_col_names(self._tablename, col_names)


        for batch in range(record_position, len(data), batch_size):
            curr_batch = data[batch:batch+batch_size]

            try:
                values = ",".join(self.cur.mogrify(parameters, i).decode('utf-8') for i in curr_batch) + ";"
                before_ds_time = perf_counter()
                self.cur.execute(query + values, curr_batch)
                self.conn.commit()

                after_ds_time = perf_counter()

                logger.info(
                    f"{after_ds_time - before_ds_time} | Inserted records from {batch} to {batch+batch_size}"
                )
            except KeyboardInterrupt:
                logger.info("User interrupted the operation.")
                print("You have interrupted the operation.")
                exit(0)

    def truncate_table(self):
        """
        This function truncates the entire table.
        """
        query = f"TRUNCATE {self._tablename};"
        sequence_reset_query = f"ALTER SEQUENCE {self._tablename}_id_seq RESTART;"

        self.cur.execute(query)
        self.cur.execute(sequence_reset_query)
        self.conn.commit()

        logger.info("The table has been truncated.")

    def get_table_continuation(self) -> int:
        """
        This function returns the number of rows in the database.
        This value is to be checked with the lines in the file, as a means
        to continue insertion from the rollback point.
        """
        query = f"SELECT last_value FROM {self._tablename}_id_seq;"

        try:
            self.cur.execute(query)
            record_position = self.cur.fetchone()
            logger.info(record_position[0])

            return int(record_position[0])
        except Exception as err:
            logger.error("Error trying to find the last_value: ", err)
            return 1

