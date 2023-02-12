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
                host="0.0.0.0",
                port=6543
            )
            cls.cur = cls.conn.cursor()
            cls._instance = super(DB, cls).__new__(cls)
        return cls._instance

    def get_connection(self):
        return self.conn

    def get_cursor(self):
        return self.cur

    def create_table(self):
        """
        This method is in-charge of creating a table in the database to input
        data into.
        """

        query = """
        CREATE TABLE IF NOT EXISTS new_sdn(
                id serial primary key,
                ts varchar(500),
                src_ip varchar(500),
                src_port varchar(500),
                dst_ip varchar(500),
                dst_port varchar(500),
                proto varchar(500),
                service varchar(500),
                duration varchar(500),
                src_bytes varchar(500),
                dst_bytes varchar(500),
                conn_state varchar(500),
                missed_bytes varchar(500),
                src_pkts varchar(500),
                src_ip_bytes varchar(500),
                dst_pkts varchar(500),
                dst_ip_bytes varchar(500),
                dns_query varchar(500),
                dns_qclass varchar(500),
                dns_qtype varchar(500),
                dns_rcode varchar(500),
                dns_AA varchar(500),
                dns_RD varchar(500),
                dns_RA varchar(500),
                dns_rejected varchar(500),
                ssl_version varchar(500),
                ssl_cipher varchar(500),
                ssl_resumed varchar(500),
                ssl_established varchar(500),
                ssl_subject varchar(500),
                ssl_issuer varchar(500),
                http_trans_depth varchar(500),
                http_method varchar(500),
                http_uri varchar(500),
                http_version varchar(500),
                http_request_body_len varchar(500),
                http_response_body_len varchar(500),
                http_status_code varchar(500),
                http_user_agent varchar(500),
                http_orig_mime_types varchar(500),
                http_resp_mime_types varchar(500),
                weird_name varchar(500),
                weird_addl varchar(500),
                weird_notice varchar(500),
                label varchar(500),
                type varchar(500)
            );
        """

        self.cur.execute(query)
        self.conn.commit()

    def batch_insert(self, data, batch_size=10000) -> None:
        """
        Batch insert records into the database.
        """

        # There is a better way to perform this operation via mogrify, but
        # this is a testing script anyway.
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        query = """
            INSERT INTO new_sdn (ts,src_ip,src_port,dst_ip,dst_port,proto,service,duration,src_bytes,dst_bytes,conn_state,missed_bytes,src_pkts,src_ip_bytes,dst_pkts,dst_ip_bytes,dns_query,dns_qclass,dns_qtype,dns_rcode,dns_AA,dns_RD,dns_RA,dns_rejected,ssl_version,ssl_cipher,ssl_resumed,ssl_established,ssl_subject,ssl_issuer,http_trans_depth,http_method,http_uri,http_version,http_request_body_len,http_response_body_len,http_status_code,http_user_agent,http_orig_mime_types,http_resp_mime_types,weird_name,weird_addl,weird_notice,label,type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

        for batch in range(0, len(data), batch_size):
            self.cur.executemany(query, data[batch : batch + batch_size])
            self.conn.commit()
        
            logger.info(f"Inserted records from {batch} to {batch+batch_size}")
