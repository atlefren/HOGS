# -*- coding: utf-8 -*-
import os
import urlparse
import datetime

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv, find_dotenv

from postgis_helpers import IteratorFile
from helpers import create_table_name


load_dotenv(find_dotenv())


class Db(object):
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
                        datasetid varchar(255),
                        version bigint,
                        geom geometry(Geometry,4326),
                        attribs jsonb,
                        test varchar(255),
                        FOREIGN KEY(datasetid, version) REFERENCES adm.datasetstore(dataset_id, version)
                    );
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier('datastore'),
                    sql.Literal(schema_name),
                )
            )
            self.conn.commit()

            cur.execute(
                sql.SQL("""
                    CREATE INDEX {} ON {}.{} (datasetid);
                """).format(
                    sql.Identifier('datastore_datasetid_idx'),
                    sql.Identifier(schema_name),
                    sql.Identifier('datastore')
                )
            )

            cur.execute(
                sql.SQL("""
                    CREATE INDEX {} ON {}.{} (version);
                """).format(
                    sql.Identifier('datastore_version_idx'),
                    sql.Identifier(schema_name),
                    sql.Identifier('datastore')
                )
            )

            cur.execute(
                sql.SQL("""
                    CREATE INDEX {} ON {}.{} USING GIST (geom);
                """).format(
                    sql.Identifier('datastore_geom_idx'),
                    sql.Identifier(schema_name),
                    sql.Identifier('datastore')
                )
            )

            self.conn.commit()

    def dataset_exists(self, schema_name, dataset_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT dataset_id FROM adm.datasetstore WHERE dataset_id = %s AND schema = %s;
            """, (dataset_id, schema_name))
            exists = cur.fetchone() is not None
            return exists

    def _create_field(self, field):
        return sql.SQL('{}->>{} as {}').format(
            sql.Identifier('attribs'),
            sql.Literal(field['normalized']),
            sql.Identifier(field['normalized']),
        )

    def _create_jsonb_query(self, fields):
        fixed = [
            sql.Identifier('id'),
            sql.Identifier('geom')
        ]

        return fixed + [self._create_field(field) for field in fields]

    def create_dataset_view(self, schema_name, dataset_id, name, version, fields):
        with self.conn.cursor() as cur:
            sql_str = sql.SQL("""
                    CREATE OR REPLACE VIEW {}.{} AS SELECT {} FROM {}.{} WHERE datasetid = {} AND version = {};
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(create_table_name(name)),
                    sql.SQL(', ').join(self._create_jsonb_query(fields)),
                    sql.Identifier(schema_name),
                    sql.Identifier('datastore'),
                    sql.Literal(dataset_id),
                    sql.Literal(version)
                )
            cur.execute(
                sql_str
            )
            self.conn.commit()

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

    def get_dataset_version(self, schema_name, dataset_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT max(version) FROM adm.datasetstore WHERE dataset_id = %s AND schema = %s;
            """, (dataset_id, schema_name))
            version = cur.fetchone()
            if version is None:
                return None
            return version[0]

    def write_features(self, schema, dataset_id, version, properties, records):
        columns = ('datasetid', 'version', 'geom', 'attribs')
        start = datetime.datetime.now()
        f = IteratorFile(dataset_id, version, records)
        with self.conn.cursor() as cur:
            cur.copy_from(f, '%s.%s' % (schema, 'datastore'), columns=columns)
        self.conn.commit()
        elapsed = datetime.datetime.now()
        print 'number of rows copied : %s' % (f._count)
        print 'time spent on copy    : %s' % (elapsed - start)
