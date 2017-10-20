# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
from multiprocessing import Queue, Lock, Array, Pool, current_process, Process, RawValue
import datetime
import logging
import random
import os

#from importer.postgis import JsonbDb
from gdal import SpatialRef
from gdal import OgrFile


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


def do_import_singlefile(worker_id, database, file, out_srs, num_imported):
    version = file['version']
    fields = file['fields']
    dataset_id = file['dataset_id']
    schema = file['schema']
    driver = file['driver']
    filename = file['file']

    logging.info('[%s] Start import %s to %s.%s' % (worker_id, filename, schema, dataset_id))

    def loop_files():
        file_obj = OgrFile(filename, driver, dataset_id, version)
        filename_base = os.path.basename(filename)
        for feature in file_obj.features:
            d = feature.to_db(out_srs)
            d['filename'] = filename_base
            yield d

    num_records = database.write_features(schema, dataset_id, version, fields, loop_files())
    num_imported.increment()
    logging.info('[%s] Copied %s records from %s to %s.%s' % (worker_id, num_records, filename, schema, dataset_id))


def worker_singlefile(file_queue, conn_str, db_class, out_srs, num_imported):
    worker_id = current_process().name
    database = db_class(conn_str)
    while not file_queue.empty():
        do_import_singlefile(worker_id, database, file_queue.get(), out_srs, num_imported)


def import_paralell_by_files(datasets, num_threads, database, db_class, out_srid):

    logging.info('[DEFAULT] Paralell by files, %s threads' % num_threads)

    out_srs = SpatialRef(out_srid)
    start = datetime.datetime.now()
    files = []
    for dataset in datasets:
        fields = OgrFile(dataset['files'][0], dataset['driver'], dataset['dataset_id'], None).fields
        dataset = database.prepare(dataset, fields)

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

    files.sort(key=lambda f: os.stat(f['file']).st_size, reverse=True)

    for file in files:
        file_queue.put(file)

    jobs = []
    for i in range(num_threads):
        p = Process(target=worker_singlefile, args=(file_queue, database.conn_str, db_class, out_srs, num_imported))
        jobs.append(p)
        p.start()

    imported_counter = 0
    num_files = len(files)
    while imported_counter < num_files:
        if num_imported.value != imported_counter:
            imported_counter = num_imported.value
            logging.info('[DEFAULT] Imported %s of %s files' % (imported_counter, num_files))
        if imported_counter == num_files:
            break

    for p in jobs:
        p.join()

    for dataset in datasets:
        database.finish(dataset, fields)
