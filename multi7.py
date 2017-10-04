# -*- coding: utf-8 -*-
from multiprocessing import Process, Queue
import multiprocessing
import os
import pickle
import math

from importer import Dataset
from importer.FileMulti import File


class PickalableSWIG:

    def __setstate__(self, state):
        self.__init__(*state['args'])

    def __getstate__(self):
        return {'args': self.args}


class PickalableDataset(Dataset, PickalableSWIG):

    def __init__(self, *args):
        self.args = args
        Dataset.__init__(self, *args)


files2 = [
    "/mnt/d/data/dummy/1.sos",
    "/mnt/d/data/dummy/2.sos"
]

files3 = [
    "/mnt/d/data/dummy/1.geojson",
    "/mnt/d/data/dummy/2.geojson"
]

datasets = [
    PickalableDataset('arealdekke1', 1, files3)
]


batch_size = 5

batches = []
for dataset in datasets:
    num_batches = int(math.ceil(float(dataset.num_features) / float(batch_size)))
    #print dataset, dataset.num_features, num_batches
    batches += [{'ds': dataset, 'start': i * batch_size, 'num': batch_size} for i in range(0, num_batches)]


q = Queue()
for b in batches:
    q.put(b)


def handle_task(q):
    pid = multiprocessing.current_process().name
    while True:
        try:
            if q.empty():
                break
            elem = q.get()
            records = elem['ds'].get_records(elem['start'], elem['num'])
            for r in records:
                print pid, r.attributes['name']
            return
        except Exception:
            continue
    return


def get_pool(input_queue):
    processes = []
    for i in range(0, 7):
        p = Process(target=handle_task, args=(input_queue,))
        p.name = 'p%s' % i
        print 'start %s' % p.name
        p.start()
        processes.append(p)
    return processes

print len(batches)

pool = get_pool(q)

for p in pool:
    p.join()

print q.empty()

#for r in ds.get_records(6, 6):
#    print r.attributes['name']

