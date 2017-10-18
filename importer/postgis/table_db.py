# -*- coding: utf-8 -*-
from psycopg2 import sql

from importer.postgis import IteratorFile
from importer.postgis import get_line_formatter
from base_db import BaseDb


class TableDb(BaseDb):

    _datasetstore_sql = '''
        CREATE TABLE adm.datasetstore (
            dataset_id varchar(255) not null,
            name varchar(255) not null,
            schema varchar(100) not null,
            version bigint not null,
            created timestamp WITH TIME ZONE not null,
            is_imported boolean not null  DEFAULT FALSE,
            table_name varchar(255),
            PRIMARY KEY (dataset_id, version)
        );
    '''

    def __init__(self, conn_str):
        super(TableDb, self).__init__(conn_str)

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

    def _get_import_table_name(self, schema, dataset_id, version):
        return '%s_%s_%s' % (schema, dataset_id, version)

    def create_import_table(self, schema, dataset_id, version, fields):
        self.create_schema('import')

        import_table_name = self._get_import_table_name(schema, dataset_id, version)
        with self.conn.cursor() as cur:
            sql_str = sql.SQL("""
                CREATE TABLE import.{} (
                    id bigserial PRIMARY KEY,
                    {},
                    geom geometry(Geometry,4326),
                    valid boolean,
                    invalid_reason varchar(255),
                    filename varchar(255)
                );
            """).format(
                sql.Identifier(import_table_name),
                sql.SQL(', ').join([self._create_field(field) for field in fields])
            )

            cur.execute(
                sql_str
            )
            self.conn.commit()

    def _create_field(self, field):
        return sql.SQL('{} {}').format(
            sql.Identifier(field['normalized']),
            sql.SQL(field['pg_type'])
        )

    def write_features(self, schema, dataset_id, version, fields, records):
        columns = [field['normalized'] for field in fields] + ['geom', 'valid', 'filename']
        f = IteratorFile(records, get_line_formatter(columns))
        import_table_name = self._get_import_table_name(schema, dataset_id, version)
        with self.conn.cursor() as cur:
            cur.copy_from(f, '%s.%s' % ('import', import_table_name), columns=tuple(columns))
        self.conn.commit()
        return f._count

    def _move(self, cur, old_schema, old_name, new_schema, new_name):
        cur.execute(sql.SQL("""
            ALTER TABLE {}.{}
            RENAME TO {}
        """).format(
            sql.Identifier(old_schema),
            sql.Identifier(old_name),
            sql.Identifier(new_name)
        ))
        cur.execute(sql.SQL("""
            ALTER TABLE {}.{}
            SET SCHEMA {}
        """).format(
            sql.Identifier(old_schema),
            sql.Identifier(new_name),
            sql.Identifier(new_schema)
        ))

    def _update_datasetstore(self, cur, dataset_id, version, schema, name):
        cur.execute(
            '''
                UPDATE adm.datasetstore
                SET table_name = %(table_name)s
                WHERE dataset_id=%(dataset_id)s
                AND version=%(version)s;
            ''',
            {'dataset_id': dataset_id, 'version': version, 'table_name': '%s.%s' % (schema, name)}
        )

    def move_table(self, schema, dataset_id, version):

        new_version = self.check_table_exists(schema, dataset_id)
        if new_version:
            self.create_schema('archive')
            prev_version_name = self._get_import_table_name(schema, dataset_id, version - 1)
        import_table_name = self._get_import_table_name(schema, dataset_id, version)
        with self.conn.cursor() as cur:
            if new_version:
                self._move(cur, schema, dataset_id, 'archive', prev_version_name)
                self._update_datasetstore(cur, dataset_id, version - 1, 'archive', prev_version_name)

            self._move(cur, 'import', import_table_name, schema, dataset_id)
            self._update_datasetstore(cur, dataset_id, version, schema, dataset_id)
            self.conn.commit()

    def add_indicies(self, schema, dataset_id, version, extra_indicies):

        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    CREATE INDEX {} ON {}.{} USING GIST (geom);
                """).format(
                    sql.Identifier('%s_geom_idx' % self._get_import_table_name(schema, dataset_id, version)),
                    sql.Identifier(schema),
                    sql.Identifier(dataset_id)
                )
            )
            for column in extra_indicies:
                self._create_index(cur, schema, dataset_id, version, column)
            self.conn.commit()

    def _create_index(self, cur, schema, dataset_id, version, column):
        cur.execute(
            sql.SQL("""
                CREATE INDEX {} ON {}.{} ({});
            """).format(
                sql.Identifier('%s_%s_idx' % (self._get_import_table_name(schema, dataset_id, version), column)),
                sql.Identifier(schema),
                sql.Identifier(dataset_id),
                sql.Identifier(column)
            )
        )
