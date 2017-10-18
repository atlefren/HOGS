# -*- coding: utf-8 -*-
import urlparse
import datetime

import psycopg2
from psycopg2 import sql
from base_db import BaseDb
from importer.postgis import IteratorFile
from importer.postgis import format_line
from importer.helpers import create_table_name


class JsonbDb(BaseDb):
    def __init__(self, conn_str):
        super(JsonbDb, self).__init__(conn_str)

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
            cur.execute("""
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
            """)
            self.conn.commit()

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

    def write_features(self, schema, dataset_id, version, properties, records):
        columns = ('datasetid', 'version', 'geom', 'attribs', 'filename', 'valid')
        f = IteratorFile(records, format_line)
        with self.conn.cursor() as cur:
            cur.copy_from(f, '%s.%s' % (schema, 'datastore'), columns=columns)
        self.conn.commit()
        return f._count
