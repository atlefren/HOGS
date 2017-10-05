# -*- coding: utf-8 -*-

from multiprocessing import Process, Queue, current_process, Pool

'''
def worker(input_queue, output_queue, func):
    pid = current_process().name
    while True:
        try:
            if input_queue.empty():
                break
            elem = input_queue.get()
            print elem
            res = func(elem)
            output_queue.put(res)
            return
        except Exception:
            continue
    return



def get_pool(input_queue, output_queue, func):
    processes = []
    for i in range(0, 7):
        p = Process(target=worker, args=(input_queue, output_queue, func))
        p.name = 'p%s' % i
        #print 'start %s' % p.name
        p.start()
        processes.append(p)
    return processes
'''

from collections import deque
from importer.gdal_multi.OgrFile import OgrFile


def worker(input_queue, output_queue, func):
    while True:
        item = input_queue.get(True)
        res = func(item)
        output_queue.put(res)


def handle(data):
    num_items = 9
    handled_items = 0
    files = deque(data['files'])
    res = []
    while handled_items < num_items:
        file = OgrFile(files.popleft(), 'GeoJSON', data['id'])
        for feature in file.features:
            handled_items += 1

    data['files'] = list(files)
    if data.get('num_handled', None) is None:
        data['num_handled'] = 0
    data['num_handled'] += handled_items

    return data


data = [
    {
        'id': 1,
        'files': [
            "/mnt/d/data/dummy/1.geojson",
            "/mnt/d/data/dummy/2.geojson",
            "/mnt/d/data/dummy/3.geojson",
        ]
    },
    {
        'id': 2,
        'files': [
            "/mnt/d/data/dummy/1.geojson",
            "/mnt/d/data/dummy/3.geojson",
        ]
    }
]


input_queue = Queue()
output_queue = Queue()

results = []

pool = Pool(3, worker, (input_queue, output_queue, handle))

for d in data:
    input_queue.put(d)

completed = []

while len(data) > len(completed):
    el = output_queue.get()

    if len(el['files']) > 0:
        input_queue.put(el)
    else:
        completed.append(el)

print 'completed'
print completed
    #remaining = el.get('remaining', [])
    #handled = el.get('handled', [])
    #results.append(handled)
    #if len(remaining) > 0:
    #    print 're-add', remaining
    #    input_queue.put(remaining)

