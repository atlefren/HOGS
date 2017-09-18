# -*- coding: utf-8 -*-
import os
import urlparse

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class Db(object):
    """docstring for Db"""
    def __init__(self,):
        result = urlparse.urlparse(os.environ.get('DATABASE_URI', None))
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port
        self.conn = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )

    def check_schema_exists(self, schema_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;
            """, (schema_name, ))
            return cur.fetchone() is not None

    def create_schema(self, schema_name):
        if self.check_schema_exists(schema_name):
            return
        with self.conn.cursor() as cur:
            cur = self.conn.cursor()
            cur.execute(
                sql.SQL("""
                    CREATE SCHEMA {};
                """).format(
                    sql.Identifier(schema_name)
                )
            )
            self.conn.commit()
            cur.execute(
                sql.SQL("""
                    CREATE TABLE {}.{} (
                        id bigserial PRIMARY KEY,
                        datasetid varchar(255) references adm.datasetstore(dataset_id),
                        geom geometry(Geometry,4326),
                        attribs jsonb
                    );
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier('datastore')
                )
            )
            self.conn.commit()

