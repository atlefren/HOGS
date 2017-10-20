# -*- coding: utf-8 -*-
import datetime
import logging

from database import JsonbDb, TableDb
from import_datasets import import_datasets

logging.basicConfig(level=logging.DEBUG)

LAYOUTS = {
    'jsonb':  JsonbDb,
    'tables': TableDb
}


def do_import(config):
    start = datetime.datetime.now()

    data_layout = config.get('data_layout', None)
    paralell_type = config.get('paralell_type', 'none')
    db_url = config['database']
    num_datasets = len(config['datasets'])
    num_threads = config.get('threads', 1)
    out_srs = config.get('out_srs', 4326)

    logging.info('[DEFAULT] Import to %s' % data_layout)
    logging.info('[DEFAULT] Import %s datasets' % num_datasets)
    # logging.info('[DEFAULT] Use %s threads' % num_threads)

    if data_layout not in LAYOUTS.keys():
        logging.warn('[DEFAULT] Data layout %s is unknown' % data_layout)
    else:
        db = LAYOUTS[data_layout](config.get('database', None))
        db.init_db(config)
        import_datasets(config['datasets'], db, LAYOUTS[data_layout], paralell_type, out_srs, num_threads)
    elapsed = datetime.datetime.now()
    logging.info('[DEFAULT] Time spent importing: %s ' % (elapsed - start))