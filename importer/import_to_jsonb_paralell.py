# -*- coding: utf-8 -*-
import datetime
import uuid
import os
import logging
import math
from multiprocessing import Process, Queue, current_process, Pool
from collections import defaultdict

from importer.gdal_multi import SpatialRef
from importer.postgis import JsonbDb


from importer.gdal_multi.OgrFile import PickalableOgrFile


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


def init_file(file):
    file.init()
    print 'inited', file.filename
    return file


def create_batch_obj(dataset, num_features, batch_size, num_batches, batch_num):
    return {
        'batch_id': str(uuid.uuid4()),
        'dataset': dataset,
        'start': batch_num * batch_size,
        'num': min(batch_size, num_features)
    }


def create_batch_queue(datasets, batch_size, num_threads):

    p = Pool(num_threads)

    files = []
    for dataset in datasets:
        files = []
        for filename in dataset['files']:
            file = PickalableOgrFile(filename, dataset['driver'], dataset['dataset_id'])
            files.append(file)
        dataset['files'] = files

    flattened_files = []
    for dataset in datasets:
        flattened_files += dataset['files']
    p.map(init_file, flattened_files)

    batches = []
    for dataset in datasets:
        num_features = sum(file.num_features for file in dataset['files'])
        num_batches = int(math.ceil(float(num_features) / float(batch_size)))
        batches += [create_batch_obj(dataset, num_features, batch_size, num_batches, batch_num) for batch_num in range(0, num_batches)]
    return batches


def get_start_file(files, start):
    idx = 0
    for i, file in enumerate(files):
        if idx <= start < idx + file.num_features:
            return i, start - idx
        idx += file.num_features


def loop_files(files, start, num_records, out_srs):
    file_idx, file_start = get_start_file(files, start)

    c = 0
    for i, file in enumerate(files[file_idx:]):
        if c >= num_records:
            break
        if i > 0:
            file_start = 0
        for r in file.get_records(file_start, num_records):
            yield r.to_db(out_srs)
            c += 1
            if c >= num_records:
                return


def import_batch(db, batch, out_srs, worker='w0'):

    dataset = batch['dataset']
    records = loop_files(dataset['files'], batch['start'], batch['num'], out_srs)
    fields = dataset['files'][0].fields
    num_records = db.write_features(dataset['schema'], dataset['dataset_id'], dataset['version'], fields, records)
    logging.info('[%s] Copied %s records to %s.%s' % (worker, num_records, dataset['schema'], dataset['dataset_id']))
    return {
        'num_records': num_records,
        'dataset_id': dataset['dataset_id'],
        'batch_id': batch['batch_id']
    }


def worker(input_queue, output_queue, db, out_srs):
    worker = current_process().name
    while True:
        try:
            if input_queue.empty():
                break
            batch = input_queue.get()
            output_queue.put(import_batch(db, batch, out_srs, worker=worker))
            return
        except Exception:
            continue
    return


def get_pool(input_queue, output_queue, db, out_srs, num_threads):
    processes = []
    for i in range(0, num_threads):
        p = Process(target=worker, args=(input_queue, output_queue, db, out_srs))
        p.name = 'p%s' % i
        p.start()
        processes.append(p)
    return processes


class BatchCounter(object):

    def __init__(self, batches):
        datasets = defaultdict(list)
        killlist = defaultdict(list)
        for batch in batches:
            datasets[batch['dataset']['dataset_id']].append(batch['batch_id'])
            killlist[batch['dataset']['dataset_id']].append(batch['batch_id'])
        self.datasets = datasets
        self.killlist = killlist
        self.finished = defaultdict(list)

    def is_finished(self, dataset_id):
        return len(self.killlist[dataset_id]) == 0

    def set_finished(self, res):
        self.finished[res['dataset_id']].append({'id': res['batch_id'], 'num': res['num_records']})
        self.killlist[res['dataset_id']].remove(res['batch_id'])

    def is_waiting(self):
        return sum([len(ids) for ids in self.killlist.values()]) > 0

    def num_imported(self, dataset_id):
        return sum([res['num'] for res in self.finished[dataset_id]])


def finish_dataset(db, dataset):
    db.mark_dataset_imported(dataset['schema'], dataset['dataset_id'], dataset['version'])


def find_dataset(datasets, dataset_id):
    return next(dataset for dataset in datasets if dataset['dataset_id'] == dataset_id)


def import_datasets(datasets, num_threads=1, database=None, out_srid=4326):

    out_srs = SpatialRef(out_srid)
    start = datetime.datetime.now()
    for dataset in datasets:
        dataset = prepare_database_for_dataset(database, dataset)

    batches = create_batch_queue(datasets, 100000, num_threads)

    batch_counter = BatchCounter(batches)
    input_queue = Queue()
    for b in batches:
        input_queue.put(b)

    out_queue = Queue()
    elapsed = datetime.datetime.now()
    print 'created Queue %s' % (elapsed-start)

    pool = get_pool(input_queue, out_queue, database, out_srs, num_threads)

    while batch_counter.is_waiting():
        if not out_queue.empty():
            batch_res = out_queue.get()
            batch_counter.set_finished(batch_res)
            dataset_id = batch_res['dataset_id']
            if batch_counter.is_finished(dataset_id):
                dataset = find_dataset(datasets, dataset_id)
                finish_dataset(database, dataset)
                logging.info('[%s] Imported %s records to %s.%s (version %s)' % (dataset_id, batch_counter.num_imported(dataset_id), dataset['schema'], dataset_id, dataset['version']))
    for p in pool:
        p.join()
