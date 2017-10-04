# -*- coding: utf-8 -*-
import os
import zipfile
import re
from collections import defaultdict
import datetime

from importer.import_to_jsonb import import_dataset


def unzip(dir_name):
    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith('.zip'):
            file_name = os.path.join(dir_name, item)  # get full path of files
            zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
            for file in zip_ref.namelist():
                if file.endswith('.sos') or file.endswith('.SOS'):
                    zip_ref.extract(file, dir_name)
            zip_ref.close()  # close file


def get_files(dir_name):
    regex = r"\d*_N50_(\w*).sos|SOS"
    p = re.compile(regex)
    types = defaultdict(list)
    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith('.sos') or item.endswith('.SOS'):
            objtype = p.search(item).group(1).lower()
            types[objtype].append(os.path.join(dir_name, item))
    return types


if __name__ == '__main__':
    start = datetime.datetime.now()

    #import_dataset('test2', 'areal', ['/mnt/d/code/kartverksdata/dl/160792/1566_N50_Arealdekke.sos'], 'areal')

    folder = '/mnt/d/code/kartverksdata/dl/160792'
    #folder = '/mnt/d/code/kartverksdata/dl/test'
    #unzip(folder)

    elapsed = datetime.datetime.now()
    start2 = elapsed
    print 'Time spent unzipping: %s ' % (elapsed - start)

    for objtype, files in get_files(folder).iteritems():
        #if objtype == 'restriksjonsomrader':
        import_dataset('n50', objtype, files, objtype)

    #import_dataset('n50', 'hoyde', get_files(folder)['hoyde'], 'hoyde')

    elapsed = datetime.datetime.now()
    print 'Time spent importing: %s ' % (elapsed - start2)
    print 'Total time spent    : %s ' % (elapsed - start)
