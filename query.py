# -*- coding: utf-8 -*-
import sys
import urlparse
import json
import datetime

import psycopg2
from psycopg2 import sql
from osgeo import ogr


class Database(object):

    def __init__(self, conn_str):
        super(Database, self).__init__()
        result = urlparse.urlparse(conn_str)
        self.conn = psycopg2.connect(
            database=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )

    def get_intersects(self, table_name, geom):
        with self.conn.cursor() as cur:
            query = sql.SQL('''
                SELECT * FROM {}
                WHERE ST_Intersects(geom, ST_GeomFromWKB(%s, 4326))
            ''').format(
                sql.SQL(table_name)
            )

            cur.execute(query, (psycopg2.Binary(geom),))
            res = []
            for record in cur:
                res.append(record)
            return res


class TableDatabase(Database):
    def __init__(self, conn_str):
        super(TableDatabase, self).__init__(conn_str)

    def get_tables(self):
        with self.conn.cursor() as cur:
            cur.execute('SELECT dataset_id, table_name FROM adm.datasetstore')
            res = {}
            for record in cur:
                res[record[0]] = record[1]
        return res


class JsonbDatabase(Database):
    def __init__(self, conn_str):
        super(JsonbDatabase, self).__init__(conn_str)

    def get_tables(self):
        with self.conn.cursor() as cur:
            cur.execute('SELECT dataset_id, schema FROM adm.datasetstore')
            res = {}
            for record in cur:
                res[record[0]] = '%s.%s' % (record[1], record[0])
        return res



def get_wkb(feature):
    return ogr.CreateGeometryFromJson(json.dumps(feature['geometry']))


class QueryBenchmark(object):
    def __init__(self, db, geoms):
        super(QueryBenchmark, self).__init__()
        self.db = db
        self.geoms = geoms
        self.tables = db.get_tables()

    def run_benchmarks(self):
        geoms = []
        for feature in self.geoms['features']:
            geom = get_wkb(feature)
            if geom.IsValid():
                geoms.append(geom.ExportToWkb())

        print 'Checking %s datasets' % len(self.tables)
        print 'Using %s query geoms' % len(geoms)

        for dataset, table_name in self.tables.iteritems():
            print dataset
            num = 0
            for geom in geoms:
                num += len(self.db.get_intersects(table_name, geom))
            print 'Found %s intersects' % num


if __name__ == '__main__':
    conn_str = sys.argv[1]
    with open(sys.argv[2], 'r') as geom_file:
        geoms = json.loads(geom_file.read())

        #db = TableDatabase(conn_str)
        db = JsonbDatabase(conn_str)
        benchmark = QueryBenchmark(db, geoms)

        start = datetime.datetime.now()
        benchmark.run_benchmarks()
        elapsed = datetime.datetime.now()
        print 'Time spent querying: %s ' % (elapsed - start)