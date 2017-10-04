# -*- coding: utf-8 -*-
import json
from multiprocessing import Pool
from collections import defaultdict

from importer import File


def count_features(data):
    file = File(data['file'])
    return {
        'dataset': data['dataset'],
        'file': data['file'],
        'num_features': file.num_features
    }


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
    files = []
    for dataset in datasets:
        for file in dataset['files']:
            files.append({
                'dataset': dataset['dataset_id'],
                'file': file
            })

    p = Pool(7)
    counted = p.map(count_features, files)
    datasets = defaultdict(list)
    for file in counted:
        datasets[file['dataset']].append(file)

    chunks = {}
    for dataset_id, results in datasets.iteritems():
        chunks[dataset_id] = chunk_dataset(results, chunk_size)
    return chunks


if __name__ == '__main__':
    with open('n50_import.json', 'r') as infile:
        conf = json.loads(infile.read())
        chunks = chunk(conf['datasets'], 100000)
        for dataset, cs in chunks.iteritems():
            print dataset
            for c in cs:
                print c['total'], c['files']
