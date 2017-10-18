# -*- coding: utf-8 -*-
from multiprocessing import Queue, Lock, Array, Pool, current_process, Process, RawValue
import datetime
import logging
import random
import os

from importer.postgis import JsonbDb
from importer.gdal import SpatialRef
from importer.gdal.OgrFile import OgrFile
from prepare import prepare_database_for_dataset


class Counter(object):
    def __init__(self, initval=0):
        self.val = RawValue('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    @property
    def value(self):
        with self.lock:
            return self.val.value


def do_import(worker_id, database, file_queue, out_srs, num_items):
    if file_queue.empty():
        return

    initial_file = file_queue.get()

    version = initial_file['version']
    fields = initial_file['fields']
    dataset_id = initial_file['dataset_id']
    schema = initial_file['schema']
    driver = initial_file['driver']

    logging.info('[%s] Start batch to %s.%s' % (worker_id, schema, dataset_id))

    files = []

    def loop_files(initial_file, num_items):
        handled_items = 0
        while handled_items < num_items:
            file = None
            if initial_file is not None:
                file = initial_file
                initial_file = None
            elif not file_queue.empty():
                file = file_queue.get()
                if file['dataset_id'] != dataset_id:
                    file_queue.put(file)
                    file = None
            if file is None:
                break
                yield
            file_obj = OgrFile(file['file'], driver, dataset_id)
            files.append(file['file'])
            for feature in file_obj.features:
                handled_items += 1
                d = feature.to_db(out_srs)
                d['filename'] = file['file']
                yield d
    num_records = database.write_features(schema, dataset_id, version, fields, loop_files(initial_file, num_items))

    logging.info('[%s] Copied %s records to %s.%s' % (worker_id, num_records, schema, dataset_id))

    return


def worker(file_queue, conn_str, out_srs, num_items):
    worker_id = current_process().name
    database = JsonbDb(conn_str)
    while not file_queue.empty():
        do_import(worker_id, database, file_queue, out_srs, num_items)


def do_import_singlefile(worker_id, database, file, out_srs, num_imported):
    version = file['version']
    fields = file['fields']
    dataset_id = file['dataset_id']
    schema = file['schema']
    driver = file['driver']
    filename = file['file']

    # logging.info('[%s] Start import %s to %s.%s' % (worker_id, filename, schema, dataset_id))

    def loop_files():
        file_obj = OgrFile(filename, driver, dataset_id, version)
        filename_base = os.path.basename(filename)
        for feature in file_obj.features:
            d = feature.to_db(out_srs)
            d['filename'] = filename_base
            yield d

    num_records = database.write_features(schema, dataset_id, version, fields, loop_files())
    num_imported.increment()
    # logging.info('[%s] Copied %s records from %s to %s.%s' % (worker_id, num_records, filename, schema, dataset_id))


def worker_singlefile(file_queue, conn_str, out_srs, num_items, num_imported):
    worker_id = current_process().name
    database = JsonbDb(conn_str)
    while not file_queue.empty():
        do_import_singlefile(worker_id, database, file_queue.get(), out_srs, num_imported)


def import_paralell_by_files(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):
    out_srs = SpatialRef(out_srid)
    start = datetime.datetime.now()
    files = []
    for dataset in datasets:
        dataset = prepare_database_for_dataset(database, dataset)
        fields = OgrFile(dataset['files'][0], dataset['driver'], dataset['dataset_id'], dataset['version']).fields

        for file in dataset['files']:
            files.append({
                'version': dataset['version'],
                'schema': dataset['schema'],
                'fields': fields,
                'dataset_id': dataset['dataset_id'],
                'file': file,
                'driver': dataset['driver']
            })

    file_queue = Queue()
    num_imported = Counter()
    # TODO: try to import one file at a time (makes for simpler code)

    # shuffle the files so that not all processes start with the same dataset
    # random.shuffle(files)

    # TODO try to sort by filesize (desc)
    files.sort(key=lambda f: os.stat(f['file']).st_size, reverse=True)

    for file in files:
        file_queue.put(file)

    jobs = []
    for i in range(num_threads):
        p = Process(target=worker_singlefile, args=(file_queue, database.conn_str, out_srs, num_items, num_imported))
        jobs.append(p)
        p.start()

    imported_counter = 0
    num_files = len(files)
    while imported_counter < num_files:
        if num_imported.value != imported_counter:
            imported_counter = num_imported.value
            print 'Imported %s of %s files' % (imported_counter, num_files)
        if imported_counter == num_files:
            break

    for p in jobs:
        p.join()

    for dataset in datasets:
        database.create_dataset_view(dataset['schema'],  dataset['dataset_id'], dataset['dataset_name'], dataset['version'], fields)
        database.write_schema_table(dataset['schema'], dataset['dataset_id'], dataset['version'], fields)
        database.mark_dataset_imported(dataset['schema'], dataset['dataset_id'], dataset['version'])