# -*- coding: utf-8 -*-
import datetime
import uuid
import os
import logging
from multiprocessing import Queue, current_process, Pool
from collections import deque

from importer.gdal import SpatialRef
from importer.gdal.OgrFile import OgrFile
from importer.postgis import JsonbDb


def worker(input_queue, output_queue, func, num_items, conn_str, out_srs):
    worker_id = current_process().name
    database = JsonbDb(conn_str)
    while True:
        item = input_queue.get(True)
        res = func(worker_id, item, num_items, database, out_srs)
        output_queue.put(res)


def loop_files(files, num_items, out_srs, dataset):
    handled_items = 0
    while handled_items < num_items:
        try:
            file = OgrFile(files.popleft(), dataset['driver'], dataset['dataset_id'])
            for feature in file.features:
                handled_items += 1
                yield feature.to_db(out_srs)
        except IndexError:
            break


def handle_dataset(worker_id, dataset, num_items, database, out_srs):

    logging.info('[%s] Start batch to %s.%s' % (worker_id, dataset['schema'], dataset['dataset_id']))
    fields = OgrFile(dataset['files'][0], dataset['driver'], dataset['dataset_id']).fields
    files = deque(dataset['files'])

    records = loop_files(files, num_items, out_srs, dataset)

    num_records = database.write_features(dataset['schema'], dataset['dataset_id'], dataset['version'], fields, records)
    logging.info('[%s] Copied %s records to %s.%s' % (worker_id, num_records, dataset['schema'], dataset['dataset_id']))

    dataset['files'] = list(files)
    if dataset.get('num_handled', None) is None:
        dataset['num_handled'] = 0
    dataset['num_handled'] += num_records

    return dataset


def prepare_database_for_dataset(db, dataset):
    append = not dataset.get('new_version', True)

    schema = dataset['schema']
    name = dataset['dataset_name'],
    dataset_id = dataset.get('dataset_id', None)

    version = 1
    created = datetime.datetime.now()
    if dataset_id is None or not db.dataset_exists(schema, dataset_id):
        if dataset_id is None:
            dataset_id = str(uuid.uuid4())
        logging.info('[%s] Create dataset %s.%s' % (dataset_id, schema, dataset_id))
        version = 1
    elif append:
        version = db.get_dataset_version(schema, dataset_id)
        logging.info('[%s] Append to dataset %s.%s version %s' % (dataset_id, schema, dataset_id, version))
    else:
        version = db.get_dataset_version(schema, dataset_id) + 1
        logging.info('[%s] Update dataset %s.%s to version %s' % (dataset_id, schema, dataset_id, version))

    db.create_dataset(schema, dataset_id, name, version, created)
    dataset['version'] = version
    dataset['schema'] = schema
    return dataset


def mark_done(dataset, database):
    database.mark_dataset_imported(dataset['schema'], dataset['dataset_id'], dataset['version'])


def import_datasets(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):

    out_srs = SpatialRef(out_srid)
    start = datetime.datetime.now()
    for dataset in datasets:
        dataset = prepare_database_for_dataset(database, dataset)

    input_queue = Queue()
    output_queue = Queue()

    pool = Pool(num_threads, worker, (input_queue, output_queue, handle_dataset, num_items, database.conn_str, out_srs))

    for dataset in datasets:
        input_queue.put(dataset)

    completed = []
    while len(datasets) > len(completed):
        dataset = output_queue.get()

        if len(dataset['files']) > 0:
            input_queue.put(dataset)
        else:
            mark_done(dataset, database)
            completed.append(dataset)
            logging.info('[%s] Imported %s records to %s.%s (version %s)' % (dataset['dataset_id'], dataset['num_handled'], dataset['schema'], dataset['dataset_id'], dataset['version']))
