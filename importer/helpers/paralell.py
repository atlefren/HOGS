# -*- coding: utf-8 -*-
from multiprocessing import Process, Queue


def handle_task(input_queue, output_queue, func):
    while not input_queue.empty():
        param = input_queue.get()
        res = func(param)
        output_queue.put(res)


def create_pool(num, func, input_queue, output_queue):
    processes = []
    for i in range(0, num):
        p = Process(target=handle_task, args=(input_queue, output_queue, func))
        p.start()
        processes.append(p)
    return processes


def paralellize(input_elems, func, num_processes):
    input_queue = Queue()
    for item in input_elems:
        input_queue.put(item)
    output_queue = Queue()
    num_threads = num_processes if len(input_elems) > num_processes else len(input_elems)
    pool = create_pool(num_threads, func, input_queue, output_queue)
    for p in pool:
        p.join()
    result = []
    while not output_queue.empty():
        result.append(output_queue.get())
    return result
