# -*- coding: utf-8 -*-
import os
from osgeo import ogr
import magic

from OgrLayer import OgrLayer
from importer.helpers import create_table_name


def get_encoding(filename):
    blob = open(filename).read()
    m = magic.Magic(mime_encoding=True)
    encoding = m.from_buffer(blob)
    return encoding


def convert_file(filename):
    name, file_extension = os.path.splitext(filename)
    base = os.path.basename(filename)
    if file_extension.lower() != '.sos':
        return filename
    if get_encoding(filename) != 'utf-8':
        return filename

    isopath = os.path.join(os.path.dirname(filename), 'iso88591')
    if not os.path.exists(isopath):
        os.makedirs(isopath)
    new_filename = os.path.join(isopath, base)
    with open(filename, 'r') as infile:
        with open(new_filename, 'wb') as outfile:
            outfile.write(unicode(infile.read(), 'utf-8-sig').encode('latin-1', 'replace'))
            return new_filename


def uniq(list_of_dicts, key):
    seen_values = set()
    without_duplicates = []
    for d in list_of_dicts:
        value = d[key]
        if value not in seen_values:
            without_duplicates.append(d)
            seen_values.add(value)
    return without_duplicates


class OgrFile(object):

    def __init__(self, filename, driver_name, dataset_id, version):
        self.filename = filename
        self.dataset_id = dataset_id
        self.version = version
        self.encoding = None
        self._driver = ogr.GetDriverByName(str(driver_name))
        self._num_features = None
        self._layers = None
        self._features = None
        self._source = None

    @property
    def source(self):
        if self._source is None:
            filename = convert_file(self.filename)
            self.encoding = get_encoding(filename)
            self._source = self._driver.Open(filename, 0)
        return self._source

    @property
    def layers(self):
        if self._layers is None:
            num_layers = self.source.GetLayerCount()
            self._layers = [OgrLayer(self.source.GetLayerByIndex(i), self.encoding) for i in range(0, num_layers)]
        return self._layers

    @property
    def num_features(self):
        if self._num_features is None:
            self._num_features = sum([layer.num_features for layer in self.layers])
        return self._num_features

    @property
    def features(self):
        filename = os.path.basename(self.filename)
        for layer in self.layers:
            for feature in layer.features():
                feature.set_extra({
                    'version': self.version,
                    'dataset_id': self.dataset_id,
                    'filename': filename
                })
                yield feature

    @property
    def fields(self):
        fields = []
        for layer in self.layers:
            fields += layer.schema
        return uniq(fields, 'name')

    def __del__(self):
        if self._source is not None:
            self._source.Destroy()
            self._source = None
