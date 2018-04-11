import math
import numpy as np
import matplotlib.pyplot as plt
import datetime


def to_timedelta(duration):
    parts = duration.split(':')
    hours = float(parts[0])
    mins = float(parts[1])
    secs = float(parts[2])
    return datetime.timedelta(seconds=((hours * 60.0 * 60.0) + (mins * 60.0) + secs))


def to_seconds(duration):
    return duration.total_seconds()


def to_minutes(duration):
    return duration.total_seconds() / 60.0


def get_duration(lines):
    return [to_timedelta(l.split(': ')[1]) for l in lines]


def avg(nums):
    return sum(nums) / len(nums)


def summarize(files, operation):
    for file in files:
        with open(file) as infile:
            times = get_duration(infile.read().splitlines())
            print file
            for t in times:
                print operation(t)
            average = avg([to_seconds(t) for t in times])
            print 'Avg: %s (%s s)' % (str(datetime.timedelta(seconds=average)), average)

print 'import'
summarize(['import_jsonb.txt', 'import_tables.txt'], to_minutes)


print 'query'
summarize(['query_jsonb.txt', 'query_tables.txt'], to_seconds)
