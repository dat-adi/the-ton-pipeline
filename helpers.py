#!/usr/bin/python3
"""
This is the file that holds helper functions for the database operations in 
Python.

@author: G V Datta Adithya
"""


def create_table_from_header(table_name, header):
    """
    This function reads from the header and produces a CREATE_TABLE_QUERY.
    """

    query = f"""CREATE TABLE IF NOT EXISTS {table_name}(id serial primary key, """
    headers = header.split(",")

    for attribute in headers[:-1]:
        query += attribute + " varchar(500), "

    query += headers[-1] + " varchar(500)"

    query += ");"

    return query


def insert_into_table_from_header(table_name, header):
    """
    This function reads from the header and produces a INSERT_TABLE_QUERY.
    """

    query = f"INSERT INTO {table_name}("
    parameters = ""
    headers = header.split(",")

    for attribute in headers[:-1]:
        query += attribute + ", "
        parameters += r"%s, "

    query += headers[-1]
    query += ")"

    parameters += "%s"
    query += f" VALUES ({parameters})"

    query += ";"

    return query


if __name__ == "__main__":
    print(insert_into_table_from_header("test_table", "attr_1, attr_2, attr_3, attr_4"))
