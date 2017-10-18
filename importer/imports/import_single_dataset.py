# -*- coding: utf-8 -*-
from importer.gdal import SpatialRef
from importer.gdal.OgrFile import OgrFile
from importer.postgis import JsonbDb
import logging
import datetime

from prepare import prepare_database_for_dataset


def get_iterator(dataset, out_srs):
    for filename in dataset['files']:
        file = OgrFile(filename, dataset['driver'], dataset['dataset_id'], dataset['version'])
        for feature in file.features:
            yield feature.to_db(out_srs)


def import_single_dataset(conn_str, out_srid, dataset):
    start = datetime.datetime.now()
    dataset_id = dataset['dataset_id']
    database = JsonbDb(conn_str)
    out_srs = SpatialRef(out_srid)
    dataset = prepare_database_for_dataset(database, dataset)
    fields = OgrFile(dataset['files'][0], dataset['driver'], dataset_id, None).fields
    iterator = get_iterator(dataset, out_srs)
    num_records = database.write_features(dataset['schema'], dataset_id, dataset['version'], iterator)
    database.create_dataset_view(dataset['schema'], dataset_id, dataset['dataset_name'], dataset['version'], fields)
    database.write_schema_table(dataset['schema'], dataset_id, dataset['version'], fields)
    database.mark_dataset_imported(dataset['schema'], dataset_id, dataset['version'])
    elapsed = datetime.datetime.now()
    time_spent = elapsed - start
    logging.info('[%s] Updated dataset %s.%s with %s records to version %s (took %s)' % (dataset_id, dataset['schema'], dataset_id, num_records,  dataset['version'], time_spent))
