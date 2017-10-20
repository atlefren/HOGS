# -*- coding: utf-8 -*-
import urlparse
import datetime
import uuid
import logging

import psycopg2
from psycopg2 import sql

from base_db import BaseDb
from importer.postgis import IteratorFile
from importer.postgis import get_line_formatter
from importer.helpers import create_table_name


class JsonbDb(BaseDb):

    def __init__(self, conn_str):
        super(JsonbDb, self).__init__(conn_str)

    def prepare(self, dataset, fields):
        append = not dataset.get('new_version', True)

        schema = dataset['schema']
        name = dataset['dataset_name'],
        dataset_id = dataset.get('dataset_id', None)

        version = 1
        created = datetime.datetime.now()
        if dataset_id is None or not self.dataset_exists(schema, dataset_id):
            if dataset_id is None:
                dataset_id = str(uuid.uuid4())
            logging.info('[%s] Create dataset %s.%s' % (dataset_id, schema, dataset_id))
            version = 1
        elif append:
            version = self.get_dataset_version(schema, dataset_id)
            logging.info('[%s] Append to dataset %s.%s version %s' % (dataset_id, schema, dataset_id, version))
        else:
            version = self.get_dataset_version(schema, dataset_id) + 1
            logging.info('[%s] Update dataset %s.%s to version %s' % (dataset_id, schema, dataset_id, version))

        self.create_dataset(schema, dataset_id, name, version, created)
        dataset['version'] = version
        dataset['schema'] = schema
        dataset['fields'] = fields
        return dataset

    def finish(self, dataset, fields):
        dataset_id = dataset['dataset_id']
        schema_name = dataset['schema']
        dataset_version = dataset['version']

        self.create_dataset_view(schema_name, dataset_id, dataset['dataset_name'], dataset_version, fields)
        self.write_schema_table(schema_name, dataset_id, dataset_version, fields)
        self.mark_dataset_imported(schema_name, dataset_id, dataset_version)

    def create_schema(self, schema_name):
        if self.check_schema_exists(schema_name):
            return
        super(JsonbDb, self).create_schema(schema_name)
        with self.conn.cursor() as cur:
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
                sql.SQL(
                    'DROP VIEW IF EXISTS {}.{}'
                ).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(create_table_name(name))
                )
            )

            cur.execute(
                sql_str
            )
            self.conn.commit()

    def write_features(self, schema, dataset_id, version, fields, records):
        columns = ('datasetid', 'version', 'geom', 'attribs', 'filename', 'valid')
        f = IteratorFile(records, get_line_formatter(columns))
        with self.conn.cursor() as cur:
            cur.copy_from(f, '%s.%s' % (schema, 'datastore'), columns=columns)
        self.conn.commit()
        return f._count
