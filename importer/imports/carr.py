# -*- coding: utf-8 -*-


def is_empty(arr):
    for i in range(len(arr)):
        if arr[i] is not None:
            return False
    return True


def is_full(arr):
    for i in range(len(arr)):
        if arr[i] is None:
            return False
    return True


def first_empty(arr):
    for i in range(len(arr)):
        if arr[i] is None:
            return i
    return -1


def add_to_arr(arr, lst):
    start = first_empty(arr)
    stop = start + len(lst)
    for i, el in enumerate(lst):
        try:
            arr[i + start] = el
        except IndexError as e:
            print i + start
            print print_arr(arr)
        


def print_arr(arr):
    return '[%s]' % ', '.join([arr[i] if arr[i] is not None else 'None' for i in range(len(arr))])