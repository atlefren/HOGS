# -*- coding: utf-8 -*-
from multiprocessing import Pool
import datetime
import logging

from import_to_jsonb import import_dataset as import_jsonb

from import_to_jsonb_paralell2 import import_datasets as import_jsonb_p

from importer.postgis import JsonbDb
# serial 22 s

logging.basicConfig(level=logging.DEBUG)


def import_dataset_jsonb(dataset):
    append = not dataset.get('new_version', True)
    import_jsonb(
            dataset['schema'],
            dataset['dataset_name'],
            dataset['files'],
            dataset_id=dataset.get('dataset_id', None),
            append=append,
            database=dataset['database']
        )
    return True


def do_import(config):
    start = datetime.datetime.now()

    db_url = config['database']
    num_datasets = len(config['datasets'])
    logging.info('[DEFAULT] Import %s datasets' % num_datasets)

    num_threads = config.get('threads', 1)
    if num_threads > num_datasets:
        num_threads = num_datasets

    logging.info('[DEFAULT] Use %s threads' % num_threads)
    for d in config['datasets']:
        d['database'] = config['database']

    p = Pool(num_threads)
    if config['data_layout'] == 'jsonb':
#        print config['database']
        db = JsonbDb(config['database'])
        if not db.check_adm_exists():
            logging.info('[DEFAULT] Create adm schema')
            db.create_adm_table()
        schemas = list(set([d['schema'] for d in config['datasets']]))
        for schema in schemas:
            if not db.check_schema_exists(schema):
                logging.info('[DEFAULT] Create schema %s' % (schema))
                db.create_schema(schema)

        p.map(import_dataset_jsonb, config['datasets'])
        #import_jsonb(config['datasets'], num_threads=num_threads, database=db)

    else:
        logging.warn('[DEFAULT] Data layout %s is unknown' % config['data_layout'])

    elapsed = datetime.datetime.now()
    logging.info('[DEFAULT] Time spent importing: %s ' % (elapsed - start))


def do_import_paralell(config):
    start = datetime.datetime.now()

    db_url = config['database']
    num_datasets = len(config['datasets'])
    logging.info('[DEFAULT] Import %s datasets' % num_datasets)

    num_threads = config.get('threads', 1)
    #if num_threads > num_datasets:
    #    num_threads = num_datasets

    logging.info('[DEFAULT] Use %s threads' % num_threads)
    #for d in config['datasets']:
    #    #d['database'] = config['database']

    #p = Pool(num_threads)
    if config['data_layout'] == 'jsonb':
#        print config['database']
        db = JsonbDb(config['database'])
        if not db.check_adm_exists():
            logging.info('[DEFAULT] Create adm schema')
            db.create_adm_table()
        schemas = list(set([d['schema'] for d in config['datasets']]))
        for schema in schemas:
            if not db.check_schema_exists(schema):
                logging.info('[DEFAULT] Create schema %s' % (schema))
                db.create_schema(schema)

        # old way
        #p.map(import_dataset_jsonb, config['datasets'])
        import_jsonb_p(config['datasets'], num_threads=num_threads, database=db)

    else:
        logging.warn('[DEFAULT] Data layout %s is unknown' % config['data_layout'])

    elapsed = datetime.datetime.now()
    logging.info('[DEFAULT] Time spent importing: %s ' % (elapsed - start))
