# -*- coding: utf-8 -*-
from FileMulti import File
import os
from importer.gdal_multi import SpatialRef
from importer.helpers.PickalableSWIG import PickalableSWIG


class PickalableFile(File, PickalableSWIG):

    def __init__(self, *args):
        self.args = args
        File.__init__(self, *args)


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


def create_files(filenames):

    res = []
    for filename in filenames:
        f = File(filename)
        f.num_features
        res.append(f)
    return res


class Dataset(object):

    def __init__(self, dataset_id, version, schema, files):
        self.dataset_id = dataset_id
        self.version = version
        self.schema = schema
        self.filenames = files
        self._files = None
        self._num_features = None
        self.out_srs = SpatialRef(4326)

    @property
    def files(self):
        if self._files is None:
            self._files = [PickalableFile(filename) for filename in self.filenames]
        return self._files

    @property
    def num_features(self):
        if self._num_features is None:
            self._num_features = sum([f.num_features for f in self.files])
        return self._num_features

    def fields(self):
        return self.files[0].fields()

    def get_records(self, start, num):
        for feature in self.get_features(start, num):
            if not feature.geom_valid:
                yield {
                    'valid': False,
                    'geom': feature.wkt,
                    'reason': feature.invalid_reason
                }
            else:
                feature.transform(self.out_srs)
                yield {
                    'valid': True,
                    'geom': feature.ewkb_hex,
                    'attributes': feature.attributes
                }

    def get_features(self, start, num):
        file_idx, file_start = get_start_file(self.files, start)

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

    def __repr__(self):
        return 'DS %s' % self.dataset_id


class PickalableDataset(Dataset, PickalableSWIG):

    def __init__(self, *args):
        self.args = args
        Dataset.__init__(self, *args)