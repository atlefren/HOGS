# -*- coding: utf-8 -*-
from imports_tables import import_serial


def import_datasets(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):
    import_serial(datasets, database, out_srid)
