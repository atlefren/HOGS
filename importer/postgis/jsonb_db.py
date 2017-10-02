# -*- coding: utf-8 -*-
import urlparse
import datetime

import psycopg2
from psycopg2 import sql

from importer.postgis import IteratorFile
from importer.postgis import format_line
from importer.helpers import create_table_name


class Db(object):
    def __init__(self, conn_str):
        result = urlparse.urlparse(conn_str)
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
        with self.conn.cursor() as cur:
            cur = self.conn.cursor()
            cur.execute("""
                CREATE SCHEMA adm;
            """)
            self.conn.commit()
            cur.execute("""
                CREATE TABLE adm.datasetstore (
                    dataset_id varchar(255) not null,
                    name varchar(255) not null,
                    schema varchar(100) not null,
                    version bigint not null,
                    created timestamp WITH TIME ZONE not null,
                    is_imported boolean not null  DEFAULT FALSE,
                    PRIMARY KEY (dataset_id, version)
                );
            """)
            self.conn.commit()
            cur.execute("""
                CREATE VIEW adm.datasetstore_latest AS
                SELECT DISTINCT ON (dataset_id)
                dataset_id, name, schema, version, created
                FROM adm.datasetstore
                WHERE is_imported = TRUE
                ORDER BY dataset_id, version DESC;
            """)
            self.conn.commit()

    def check_schema_exists(self, schema_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = %s;
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
                        valid boolean,
                        invalid_reason varchar(255),
                        filename varchar(255),
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
                SELECT dataset_id
                FROM adm.datasetstore
                WHERE dataset_id = %s
                AND schema = %s;
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

        return fixed + [self._create_field(field) for field in fields] + [sql.Identifier('filename')]

    def create_dataset_view(self, schema_name, dataset_id, name, version, fields):
        with self.conn.cursor() as cur:
            sql_str = sql.SQL("""
                    CREATE OR REPLACE VIEW {}.{} AS
                        SELECT {}
                        FROM {}.{}
                        WHERE datasetid = {}
                        AND version = {};
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

    def mark_dataset_imported(self, schema_name, dataset_id, version):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE adm.datasetstore
                SET is_imported = TRUE
                WHERE dataset_id=%(dataset_id)s
                AND version=%(version)s;
            """, {'dataset_id': dataset_id, 'version': version})
            self.conn.commit()

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
        columns = ('datasetid', 'version', 'geom', 'attribs', 'valid', 'invalid_reason', 'filename')
        start = datetime.datetime.now()
        f = IteratorFile(dataset_id, version, records, format_line)
        with self.conn.cursor() as cur:
            cur.copy_from(f, '%s.%s' % (schema, 'datastore'), columns=columns)
        self.conn.commit()
        elapsed = datetime.datetime.now()
        print 'number of rows copied : %s' % (f._count)
        print 'time spent on copy    : %s' % (elapsed - start)
