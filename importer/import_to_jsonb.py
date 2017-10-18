# -*- coding: utf-8 -*-

from imports import import_serial, import_paralell_by_dataset, import_paralell_by_files


def import_datasets(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):

    paralell_type = 'paralell_files'

    if paralell_type == 'none':
        import_serial(datasets, database, out_srid)
    elif paralell_type == 'paralell_ds':
        import_paralell_by_dataset(datasets, num_threads, database, out_srid)
    elif paralell_type == 'paralell_files':
        import_paralell_by_files(datasets, num_threads, database, out_srid, num_items)
