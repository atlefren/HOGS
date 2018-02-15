import math
import numpy as np
import matplotlib.pyplot as plt


def to_seconds(duration):
    parts = duration.split(':')
    hours = float(parts[0])
    mins = float(parts[1])
    secs = float(parts[2])
    return ((hours * 60.0 * 60.0) + (mins * 60.0) + secs)


def to_minutes(duration):
    return to_seconds(duration) / 60.0


def get_duration(lines, operation):
    return [operation(l.split(': ')[1]) for l in lines]


def summarize(files, operation):
    for file in files:
        with open(file) as infile:
            times = get_duration(infile.read().splitlines(), operation)
            print file
            for t in times:
                print t

print 'import'
summarize(['import_jsonb.txt', 'import_tables.txt'], to_minutes)


print 'query'
summarize(['query_jsonb.txt', 'query_tables.txt'], to_seconds)
'''
names = files
colors = ['r', 'b']
ind = np.arange(1)  # the x locations for the groups
width = 0.35       # the width of the bars
fig, ax = plt.subplots()
rects = []
for i, (key, values) in enumerate(res.iteritems()):
    means = [values['avg']]
    std = [values['std']]
    print ind + i * width, means, width
    rects.append(ax.bar(ind + i * width, means, width, color=colors[i], yerr=std))

# add some text for labels, title and axes ticks
#ax.set_ylabel('')
#ax.set_title('')
#ax.set_xticks(ind + width / 2)

#plt.xticks(np.arange(len(names)), names)

#ax.legend([r[0] for r in rects], names)


def autolabel(rects):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)

#plt.savefig("test.png")
'''