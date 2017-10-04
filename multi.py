#"from multiprocessing import Process
#from multiprocessing import Process, Queue
from multiprocessing import Pool

from importer.helpers import paralellize


def fib(n):
    if n < 2:
        return n
    return fib(n-2) + fib(n-1)

data = [30, 31, 32, 33, 34, 35, 20]
#print paralellize(data, fib, 7)

p = Pool(7)
print p.map(fib, data)

'''
def add_to_q(q, data):
    for item in data:
        q.put(item)


def my_func(queue, out_q):
    while not queue.empty():
        num = queue.get()
        if num is not None:
            fibn = fib(num)
            print 'fib of %s = %s' % (num, fibn)
            out_q.put(fibn)

in_q = Queue()
data = [30, 31, 32, 33, 34, 35, 20]
out_q = Queue()


def create_pool(num, func, in_q, out_q):
    processes = []
    for i in range(0, num):
        p = Process(target=func, args=(in_q, out_q))
        p.start()
        processes.append(p)
    return processes

add_to_q(in_q, data)

processes = create_pool(5, my_func, in_q, out_q)

for process in processes:
    process.join()
print 'done!'
while not out_q.empty():
    print out_q.get()
'''