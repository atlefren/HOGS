# -*- coding: utf-8 -*-
import os
from importer.postgis import IteratorFile
import psycopg2
import urlparse
from multiprocessing import Process, Queue

result = urlparse.urlparse('postgres://dvh2:pass@localhost:5432/dvh2')
username = result.username
password = result.password
database = result.path[1:]
hostname = result.hostname
port = result.port
conn = psycopg2.connect(
    database=database,
    user=username,
    password=password,
    host=hostname,
    port=port
)


def my_gen(id, max_n):
    n = 0
    while n < max_n:
        yield '%s: %s' % (id, n)
        n = n + 1


def gen_to_q(generator, q):
    for e in generator:
        q.put(e)


def get_queues(generators, size):
    res = []
    for g in generators:
        q = Queue()
        gen_to_q(g, q)

        #for i in range(0, n):
        '''
        q.put('i 1, q %s' % i)
        q.put('i 2, q %s' % i)
        q.put('i 3, q %s' % i)
        q.put('i 4, q %s' % i)
        q.put('i 5, q %s' % i)
        '''
        res.append(q)
    return res


def handle_task(q):
    while not q.empty():
        print 'q=%s: %s' % (os.getpid(), q.get())


def pool_queues(queues):
    processes = []
    for q in queues:
        p = Process(target=handle_task, args=(q,))
        p.start()
        processes.append(p)
    return processes

queues = get_queues([my_gen(1, 100), my_gen(2, 40), my_gen(3, 5)])

pool = pool_queues(queues, 10)

for p in pool:
    p.join()


'''
for q in queues:
    while not q.empty():
        print q.get()
'''


'''
def my_gen(max_n):
    n = 0
    while n < max_n:
        yield {'n': n}
        n = n + 1


q1 = Queue()
q1.put('test1')

q2 = Queue()
q2.put('test2')

q3 = Queue()
q3.put('test3')

datasets = [
    {'gen': my_gen(1000), 'id': 'ds1'},
    {'gen': my_gen(100), 'id': 'ds2'},
    {'gen': my_gen(40), 'id': 'ds3'},
]

datasets2 = [
    {'gen': q1, 'id': 'ds1'},
    {'gen': q2, 'id': 'ds2'},
    {'gen': q3, 'id': 'ds3'},
]


def handle_task(input_queue):
    while not input_queue.empty():
        print input_queue.get()


def create_pool(num, input_queue):
    processes = []
    for i in range(0, num):
        p = Process(target=handle_task, args=(input_queue))
        p.start()
        processes.append(p)
    return processes


input_queue = Queue()
for item in datasets2:
    input_queue.put(item)

p = create_pool(2, input_queue)

for p in pool:
        p.join()

'''
'''
def insert(conn, table, columns, generator, num, format_line, dataset_id, version):
    g2 = get_from_gen(generator, num)
    f = IteratorFile(dataset_id, version, g2, format_line)
    with conn.cursor() as cur:
        cur.copy_from(f, table, columns=columns)
    conn.commit()


def format_line(r):
    line = '%s\t%s\t%s' % (
        u'record %s' % r['n'],
        r.get('dataset_id', '\N'),
        r.get('version', '\N')
    )
    return unicode(line)


insert(conn, '%s.%s' % ('public', 'test'), ('rec', 'dataset_id', 'version'), g, 10, format_line, 'id', 2)
'''
conn.close()
