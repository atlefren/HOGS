# -*- coding: utf-8 -*-
import sys
import json
from multiprocessing import Process, Queue
from collections import defaultdict

from importer import File


def count_features(input_queue, output_queue):
    while not input_queue.empty():
        el = input_queue.get()
        file = File(el['file'])
        output_queue.put({
            'dataset': el['dataset'],
            'file': el['file'],
            'num_features': file.num_features
        })


def create_pool(num, func, input_queue, output_queue):
    processes = []
    for i in range(0, num):
        p = Process(target=func, args=(input_queue, output_queue))
        p.start()
        processes.append(p)
    return processes


def add_to_res(results, file, chunk_size):
    for res in results:
        if res['total'] + file['num_features'] <= chunk_size:
            # can fit in existing chunk
            res['total'] += file['num_features']
            res['files'].append(file['file'])
            return
    # no existing chunks, or cannot fit
    results.append({
        'total': file['num_features'],
        'files': [file['file']]
    })


def chunk_dataset(data, chunk_size):
    results = []
    for file in data:
        add_to_res(results, file, chunk_size)
    return results


def chunk(datasets, chunk_size):
    input_queue = Queue()
    c = 0
    for dataset in datasets:
        for file in dataset['files']:
            c += 1
            input_queue.put({
                'dataset': dataset['dataset_id'],
                'file': file
            })
    num_threads = 7 if c > 7 else c
    output_queue = Queue()
    pool = create_pool(num_threads, count_features, input_queue, output_queue)
    for p in pool:
        p.join()

    datasets = defaultdict(list)
    while not output_queue.empty():
        res = output_queue.get()
        datasets[res['dataset']].append(res)
    chunks = {}
    for dataset_id, results in datasets.iteritems():
        chunks[dataset_id] = chunk_dataset(results, chunk_size)
    return chunks


if __name__ == '__main__':
    with open('import_template.json', 'r') as infile:
        conf = json.loads(infile.read())
        chunks = chunk(conf['datasets'], 4000)
        for dataset, cs in chunks.iteritems():
            print dataset
            for c in cs:
                print c['total'], c['files']
        