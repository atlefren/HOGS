# -*- coding: utf-8 -*-
from multiprocessing import Pool
import datetime
import logging

from import_to_jsonb import import_datasets as import_jsonb
from importer.postgis import JsonbDb, TableDb


logging.basicConfig(level=logging.DEBUG)


def init_db(db, config):
    if not db.check_adm_exists():
        logging.info('[DEFAULT] Create adm schema')
        db.create_adm_table()
    schemas = list(set([d['schema'] for d in config['datasets']]))
    for schema in schemas:
        if not db.check_schema_exists(schema):
            logging.info('[DEFAULT] Create schema %s' % (schema))
            db.create_schema(schema)
    return db


def do_import_paralell(config):
    start = datetime.datetime.now()

    db_url = config['database']
    num_datasets = len(config['datasets'])
    num_threads = config.get('threads', 1)

    logging.info('[DEFAULT] Import %s datasets' % num_datasets)
    logging.info('[DEFAULT] Use %s threads' % num_threads)

    if config['data_layout'] == 'jsonb':
        db = JsonbDb(config['database'])
        init_db(db, config)
        import_jsonb(config['datasets'], num_threads=num_threads, database=db)
    elif config['data_layout'] == 'tables':
        db = TableDb(config['database'])
        init_db(db, config)
        import_table(config['datasets'], num_threads=num_threads, database=db)
    else:
        logging.warn('[DEFAULT] Data layout %s is unknown' % config['data_layout'])

    elapsed = datetime.datetime.now()
    logging.info('[DEFAULT] Time spent importing: %s ' % (elapsed - start))
