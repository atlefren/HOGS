# -*- coding: utf-8 -*-
from functools import partial
from multiprocessing import Pool

from import_single_dataset import import_single_dataset
from import_paralell_by_files2 import import_paralell_by_files


def import_serial(datasets, database=None, out_srid=4326):
    for dataset in datasets:
        import_single_dataset(database.conn_str, out_srid, dataset)


def import_paralell_by_dataset(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):
    len_datasets = len(datasets)
    if num_threads > len_datasets:
        num_threads = len_datasets

    p = Pool(num_threads)
    p.map(partial(import_single_dataset, database.conn_str, out_srid), datasets)
