# -*- coding: utf-8 -*-
from functools import partial
from multiprocessing import Pool
import logging

from import_single_dataset import import_single_dataset
from import_paralell_by_files import import_paralell_by_files


def import_serial(datasets, database, db_class, out_srid):
    logging.info('[DEFAULT] No paralellism')
    for dataset in datasets:
        import_single_dataset(database.conn_str, db_class, out_srid, dataset)


def import_paralell_by_dataset(datasets, num_threads, database, db_class, out_srid):
    len_datasets = len(datasets)
    if num_threads > len_datasets:
        num_threads = len_datasets

    logging.info('[DEFAULT] Paralell by dataset, %s threads' % num_threads)
    p = Pool(num_threads)
    p.map(partial(import_single_dataset, database.conn_str, db_class, out_srid), datasets)


def import_datasets(datasets, database, db_class, paralell_type, out_srid, num_threads):

    if paralell_type == 'none':
        import_serial(datasets, database, db_class, out_srid)
    elif paralell_type == 'paralell_ds':
        import_paralell_by_dataset(datasets, num_threads, database, db_class, out_srid)
    elif paralell_type == 'paralell_files':
        import_paralell_by_files(datasets, num_threads, database, db_class, out_srid)