# -*- coding: utf-8 -*-
from FileMulti import File
import os


def loop_files(files):
    for file in files:
        file = File(file)
        filename = os.path.basename(file.filename)
        for record in file.records():
            record['filename'] = filename
            yield record


def get_start_file(files, start):
    idx = 0
    for i, file in enumerate(files):
        if idx <= start < idx + file.num_features:
            return i, start - idx
        idx += file.num_features


class Dataset(object):

    def __init__(self, dataset_id, version, files):
        self.dataset_id = dataset_id
        self.version = version
        self.files = [File(file) for file in files]
        self._num_features = None

    @property
    def num_features(self):
        if self._num_features is None:
            self._num_features = sum([f.num_features for f in self.files])
        return self._num_features

    def get_records(self, start, num):
        file_idx, file_start = get_start_file(self.files, start)

        #print '%s records from %s' % (num, self.dataset_id)
        #print 'start at %s, read %s' % (self.files[file_idx], num)
        c = 0
        for i, file in enumerate(self.files[file_idx:]):
            if c >= num:
                break
            if i > 0:
                file_start = 0
            for r in file.get_records(file_start, num):
                yield r
                c += 1
                if c >= num:
                    return

        #for file in self.files[file_idx:]:
        #    print' get from ', file
        #    for r in file.get_records(file_start, num):
        #        #print '!', r
        #        yield r

    def __repr__(self):
        return 'DS %s' % self.dataset_id
