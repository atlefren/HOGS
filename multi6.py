# -*- coding: utf-8 -*-
from multiprocessing import Process, Queue
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

files1 = [
    "/mnt/d/data/dummy/1.sos"
]
files2 = [
    "/mnt/d/data/dummy/1.sos",
    "/mnt/d/data/dummy/2.sos"
]

files3 = [
    "/mnt/d/data/dummy/1.geojson",
    "/mnt/d/data/dummy/2.geojson"
]

datasets = [
    PickalableDataset('arealdekke1', 1, files3),
    #PickalableDataset('arealdekke2', 1, files2),
    #PickalableDataset('arealdekke3', 1, files3)
]


batch_size = 6

batches = []
for dataset in datasets:
    num_batches = int(math.ceil(float(dataset.num_features) / float(batch_size)))
    #print dataset, dataset.num_features, num_batches
    batches += [{'ds': dataset, 'start': i * batch_size, 'num': batch_size} for i in range(0, num_batches)]


q = Queue()
for b in batches:
    q.put(b)


def handle_task(q):
    pid = os.getpid()
    while not q.empty():
        elem = q.get()
        #print pid, elem
        records = elem['ds'].get_records(elem['start'], elem['num'])
        #for r in records:
        #    print r


def get_pool(input_queue):
    processes = []
    for i in range(0, 2):
        p = Process(target=handle_task, args=(input_queue,))
        #print 'start'
        p.start()
        processes.append(p)
    return processes


pool = get_pool(q)

for p in pool:
    p.join()


'''
f = PickalableFile(files[0])

print f.num_features


f2 = pickle.loads(pickle.dumps(f))
print f2.num_features


#ds = Dataset('arealdekke', 1, files)
'''

'''
for r in ds.get_records(10):
    print r
print '---'
for r in ds.get_records(10):
    print r

'''
'''
while ds.has_more:
    print '????'
    for r in ds.get_records(10):
        print r
'''


def fib(n):
    if n < 2:
        return n
    return fib(n-2) + fib(n-1)


def loop(num):
    for i in range(0, num):
        yield fib(i + 10)

'''
class Dataset(object):

    def __init__(self, dataset_id, version, files):
        self.id = dataset_id
        self.num_elems = version
        self.c = 1
        self.loop = loop(version)

    def get_records(self, num):
        c = 0
        while c < num:
            try:
                yield next(self.loop)
                c += 1
            except StopIteration:
                self.has_more = False
                break

    def get_records2(self, num):
        c = 0
        while c < num:
            if self.c > self.num_elems:
                break
            yield '%s: %s' % (self.id, fib(self.c + 10))
            self.c += 1
            c += 1

    @property
    def has_more(self):
        return self.c <= self.num_elems

    def __repr__(self):
        return 'DS %s' % self.id
'''
'''
datasets = [
    Dataset('arealdekke', 15, files),
    #Dataset('arealdekke2', 15, files),
    #Dataset('arealdekke3', 15, files),
]


#q = Queue()


batches = []
while len(datasets) > 0:
    d = datasets.pop()
    batch = d.get_records(12)
    if d.has_more:
        datasets.append(d)
    batches.append(batch)

for b in batches:
    print b

#for d in datasets:
#    batch = d.get_records(2)
'''


#file = FileMulti




'''

- n datasett
- med m mulige batcher a x elementer
- vet ikke hvor mange batcher pr datasett på forhånd



en pool pr datasett?
- problem: veldig forskjellig størrelse


1. prosesser alle datasett
2. 




'''









# vil ha N prosesser
# hver av disse får et dataset
    # jobber seg gjennom x elementer
    # avslutter
    # ny kø plukker opp






#q = Queue()

#for d in datasets:
#    q.put(d)

#pool = get_pool(q)



#print datasets


#for p in pool:
#    p.join()


'''
d = Dataset(1, 15)



for r in d.records(10):
    print r

print '---'

for r in d.records(10):
    print r
'''