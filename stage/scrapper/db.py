import psycopg2
import os
import requests
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient

class DatabasePostgres:
    def __init__(self, config: dict):
        self.user = config['user']
        self.password = config['password']
        self.host = config['host']
        self.port = config['port']
        self.dbname = config['dbname']
        self.connection = None

    def connect(self):
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                raise e
        return self.connection

    def get_rows(self, query, parameters=None):
        conn = self.connect()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, parameters)
            records = [row for row in cur.fetchall()]
            cur.close()
        return records

    def insert(self, query, parameters=None):
        conn = self.connect()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, parameters)
            row_id = cur.fetchone()
        conn.commit()
        return row_id

class DatabaseMongo:
    def __init__(self, config: dict):
        self.user = config['user']
        self.password = config['password']
        self.host = config['host']
        self.port = config['port']
        self.dbname = config['dbname']
        self.connection = None

    def connect(self):
        if self.connection is None:
            connection = MongoClient(
                    host = self.host,
                    port = self.port,
                    username = self.user,
                    password = self.password )
            self.connection = connection
        return self.connection

    def insert(self, collection: str, asset: list) -> None:
        conn = self.connect()
        coll = conn[self.dbname][collection]
        coll.insert_one(asset)

