# -*- coding: utf-8 -*-
import logging
import datetime

from importer.gdal import SpatialRef
from importer.gdal.OgrFile import OgrFile
from importer.postgis import TableDb
from prepare import prepare_database_for_dataset


def import_serial(datasets, database, out_srid):
    for dataset in datasets:
        import_single_dataset(database.conn_str, out_srid, dataset)


def get_iterator(dataset, out_srs):
    for filename in dataset['files']:
        file = OgrFile(filename, dataset['driver'], dataset['dataset_id'],  dataset['version'])
        for feature in file.features:
            yield feature.to_db(out_srs)


def import_single_dataset(conn_str, out_srid, dataset):
    start = datetime.datetime.now()

    dataset_id = dataset['dataset_id']
    database = TableDb(conn_str)
    out_srs = SpatialRef(out_srid)
    fields = OgrFile(dataset['files'][0], dataset['driver'], dataset_id, None).fields

    dataset = prepare_database_for_dataset(database, dataset, fields)
    schema_name = dataset['schema']
    dataset_version = dataset['version']

    iterator = get_iterator(dataset, out_srs)
    num_records = database.write_features(schema_name, dataset_id, dataset_version, fields, iterator)

    database.move_table(schema_name, dataset_id, dataset_version)
    database.add_indicies(schema_name, dataset_id, dataset_version, dataset.get('indicies', []))
    database.write_schema_table(schema_name, dataset_id, dataset_version, fields)
    database.mark_dataset_imported(schema_name, dataset_id, dataset_version)

    elapsed = datetime.datetime.now()
    time_spent = elapsed - start
    logging.info('[%s] Updated dataset %s.%s with %s records to version %s (took %s)' % (dataset_id, schema_name, dataset_id, num_records,  dataset_version, time_spent))
