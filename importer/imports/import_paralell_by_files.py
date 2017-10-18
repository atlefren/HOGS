# -*- coding: utf-8 -*-
from multiprocessing import Queue, Lock, Array, Pool, current_process, Process
from ctypes import c_char_p
import logging
import datetime
import time

import carr
from importer.gdal import SpatialRef
from importer.gdal.OgrFile import OgrFile
from importer.postgis import JsonbDb
from prepare import prepare_database_for_dataset


def get_file_from_arr(dataset, files):
    for i in range(len(files)):
        if files[i] in dataset['files']:
            file = files[i]
            files[i] = None
            return file


def do_import(worker_id, dataset, file_arr, finished_arr, lock, num_items, database, out_srs):

    logging.info('[%s] Start batch to %s.%s' % (worker_id, dataset['schema'], dataset['dataset_id']))

    files = []

    def loop_files(num_items):
        handled_items = 0

        while handled_items < num_items:
            #print handled_items, num_items
            f = get_file_from_arr(dataset, file_arr)
            if f is None:
                break
                yield
            #print worker_id, f
            #print worker_id, carr.print_arr(file_arr)
            files.append(f)
            #yield
            #handled_items += 1
            file = OgrFile(f, dataset['driver'], dataset['dataset_id'])
            #handled_items += num_items - 1
            for feature in file.features:
                handled_items += 1
                yield feature.to_db(out_srs)

    num_records = database.write_features(dataset['schema'], dataset['dataset_id'], dataset['version'], loop_files(num_items))
    carr.add_to_arr(finished_arr, files)
    logging.info('[%s] Copied %s records to %s.%s' % (worker_id, num_records, dataset['schema'], dataset['dataset_id']))
    return num_records


def do_import_dummy(worker_id, dataset, file_arr, finished_arr, lock, num_items, database, out_srs):

    logging.info('[%s] Start batch to %s.%s' % (worker_id, dataset['schema'], dataset['dataset_id']))

    files = []

    def loop_files(num_items):
        handled_items = 0
        while handled_items < num_items:
            f = get_file_from_arr(dataset, file_arr)
            if f is None:
                break
                yield
            files.append(f)
            #print worker_id, f
            file = OgrFile(f, dataset['driver'], dataset['dataset_id'])
            for feature in file.features:
                handled_items += 1
                yield feature.to_db(out_srs)
                #d['filename'] = f
                yield d

    num_records = database.write_features(dataset['schema'], dataset['dataset_id'], dataset['version'], loop_files(num_items))

    #if num_records > 0:
    logging.info('[%s] Copied %s records (%s files) to %s.%s' % (worker_id, num_records, len(files), dataset['schema'], dataset['dataset_id']))

    carr.add_to_arr(finished_arr, files)
    return num_records


def worker(ds_queue, file_arr, finished_arr, ds_arr, lock, num_items, conn_str, out_srs):
    worker_id = current_process().name
    database = JsonbDb(conn_str)
    while True:
        #print carr.print_arr(file_arr)
        dataset = ds_queue.get()
        #print dataset['dataset_id']
        ds_queue.put(dataset)
        count = do_import_dummy(worker_id, dataset, file_arr, finished_arr, lock, num_items, database, out_srs)
        if count == 0:
            carr.add_to_arr(ds_arr, [dataset['dataset_id']])
        
        if carr.is_full(ds_arr):
            print carr.print_arr(ds_arr)
            break

        #if count > 0 and not carr.is_empty(file_arr):
        #    ds_queue.put(dataset)
        #if carr.is_empty(file_arr):
        #    print '???'
        #    return


def import_paralell_by_files(datasets, num_threads=1, database=None, out_srid=4326, num_items=100000):
    out_srs = SpatialRef(out_srid)
    start = datetime.datetime.now()
    files = []
    for dataset in datasets:
        dataset = prepare_database_for_dataset(database, dataset)
        dataset['fields'] = OgrFile(dataset['files'][0], dataset['driver'], dataset['dataset_id']).fields
        files += dataset['files']

    ds_queue = Queue()
    for d in datasets:
        ds_queue.put(d)

    lock = Lock()
    #global file_arr
    file_arr = Array(c_char_p, files, lock=lock)
    #global finished_arr
    finished_arr = Array(c_char_p, len(files), lock=lock)

    ds_arr = Array(c_char_p, len(datasets), lock=lock)

    #pool = Pool(num_threads, worker, (ds_queue, file_arr, finished_arr, num_items, database.conn_str, out_srs))
    #num_items = 1
    jobs = []
    for i in range(1):
        p = Process(target=worker, args=(ds_queue, file_arr, finished_arr, ds_arr, lock, num_items, database.conn_str, out_srs))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()
    #while not carr.is_full(finished_arr):
    #    pass

