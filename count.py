# -*- coding: utf-8 -*-
import sys
import json
import os


def GetHumanReadable(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1  # increment the index of the suffix
        size = size/1024.0  # apply the division
    return "%.*f%s" % (precision, size, suffixes[suffixIndex])


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as infile:
        conf = json.loads(infile.read())
        files = []
        for dataset in conf['datasets']:
            files += dataset['files']
        sizes = sum([os.stat(f).st_size for f in files])

        print 'Num files : %s' % len(files)
        print 'Total size: %s' % GetHumanReadable(sizes)
