# -*- coding: utf-8 -*-
import urlparse
import psycopg2
from psycopg2 import sql


class BaseDb(object):

    _datasetstore_sql = '''
        CREATE TABLE adm.datasetstore (
            dataset_id varchar(255) not null,
            name varchar(255) not null,
            schema varchar(100) not null,
            version bigint not null,
            created timestamp WITH TIME ZONE not null,
            is_imported boolean not null  DEFAULT FALSE,
            PRIMARY KEY (dataset_id, version)
        );
    '''

    _datasetstore_view_sql = '''
        CREATE VIEW adm.datasetstore_latest AS
        SELECT DISTINCT ON (dataset_id)
        dataset_id, name, schema, version, created
        FROM adm.datasetstore
        WHERE is_imported = TRUE
        ORDER BY dataset_id, version DESC;
    '''

    _schema_table_sql = '''
        CREATE TABLE adm.schema (
            dataset_id varchar(255) not null,
            schema varchar(255) not null,
            version bigint not null,
            column_name_original varchar(255) not null,
            column_name varchar(255) not null,
            column_type varchar(255) not null,
            PRIMARY KEY (dataset_id, version, column_name),
            FOREIGN KEY(dataset_id, version) REFERENCES adm.datasetstore(dataset_id, version)
        )
    '''

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
        return self.check_table_exists('adm', 'datasetstore')

    def create_adm_table(self):
        with self.conn.cursor() as cur:
            cur = self.conn.cursor()
            cur.execute("""
                CREATE SCHEMA adm;
            """)
            self.conn.commit()
            cur.execute(self._datasetstore_sql)
            self.conn.commit()
            cur.execute(self._datasetstore_view_sql)
            self.conn.commit()
            cur.execute(self._schema_table_sql)
            self.conn.commit()

    def check_schema_exists(self, schema_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = %s;
            """, (schema_name, ))
            return cur.fetchone() is not None

    def check_table_exists(self, schema_name, table_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name =  %s
                );
            """, (schema_name, table_name))
            return cur.fetchone()[0]

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

    def dataset_exists(self, schema_name, dataset_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT dataset_id
                FROM adm.datasetstore
                WHERE dataset_id = %s
                AND schema = %s;
            """, (dataset_id, schema_name))
            exists = cur.fetchone() is not None
            return exists

    def create_dataset(self, schema_name, dataset_id, name, version, created):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO adm.datasetstore (dataset_id, name, schema, version, created)
                VALUES (%(dataset_id)s, %(name)s, %(schema_name)s, %(version)s, %(created)s)
            """, {
                    'dataset_id': dataset_id,
                    'schema_name': schema_name,
                    'name': name,
                    'version': version,
                    'created': created
                }
            )
            self.conn.commit()

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

    def mark_dataset_imported(self, schema_name, dataset_id, version):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE adm.datasetstore
                SET is_imported = TRUE
                WHERE dataset_id=%(dataset_id)s
                AND version=%(version)s;
            """, {'dataset_id': dataset_id, 'version': version})
            self.conn.commit()

    def write_schema_table(self, schema_name, dataset_id, version, fields):

        data = []
        for field in fields:
            data.append({
                'dataset_id': dataset_id,
                'schema':  schema_name,
                'version':  version,
                'schema':  schema_name,
                'column_name': field['normalized'],
                'column_name_original':  field['name'],
                'column_type': field['pg_type']
            })

        with self.conn.cursor() as cur:
            sql = """
                INSERT INTO
                    adm.schema (
                        dataset_id,
                        schema,
                        version,
                        column_name,
                        column_name_original,
                        column_type
                    )
                VALUES
                    (
                        %(dataset_id)s,
                        %(schema)s,
                        %(version)s,
                        %(column_name)s,
                        %(column_name_original)s,
                        %(column_type)s
                    )
            """
            cur.executemany(sql, data)
            self.conn.commit()