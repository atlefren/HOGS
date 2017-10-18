# -*- coding: utf-8 -*-
import urlparse
import psycopg2


class BaseDb(object):
    def __init__(self, conn_str):
        result = urlparse.urlparse(conn_str)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port
        self.conn_str = conn_str
        self.conn = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )

    def check_adm_exists(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'adm'
                    AND table_name = 'datasetstore'
                );
            """)
            return cur.fetchone()[0]

    def create_adm_table(self):
        pass

    def check_schema_exists(self, schema_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = %s;
            """, (schema_name, ))
            return cur.fetchone() is not None

    def create_schema(self, schema_name):
        pass

    def dataset_exists(self, schema_name, dataset_id):
        pass

    def create_dataset(self, schema_name, dataset_id, name, version, created):
        pass

    def mark_dataset_imported(self, schema_name, dataset_id, version):
        pass

    def get_dataset_version(self, schema_name, dataset_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT max(version)
                FROM adm.datasetstore
                WHERE dataset_id = %s
                AND schema = %s;
            """, (dataset_id, schema_name))
            version = cur.fetchone()
            if version is None:
                return None
            return version[0]

    def write_features(self, schema, dataset_id, version, properties, records):
        pass
