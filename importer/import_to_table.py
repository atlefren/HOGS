# -*- coding: utf-8 -*-
from imports_tables import import_serial, import_paralell_by_dataset


def import_datasets(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):

    paralell_type = 'none'

    if paralell_type == 'none':
        import_serial(datasets, database, out_srid)
    elif paralell_type == 'paralell_ds':
        import_paralell_by_dataset(datasets, num_threads, database, out_srid)
    else:
        print 'unknown type %s' % paralell_type