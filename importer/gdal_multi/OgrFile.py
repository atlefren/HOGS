# -*- coding: utf-8 -*-

from osgeo import ogr
from OgrLayer import OgrLayer


def get_start_layer(layers, start):
    idx = 0
    for i, layer in enumerate(layers):
        if idx <= start < idx + layer.num_features:
            return i, start - idx
        idx += layer.num_features


class OgrFile(object):

    def __init__(self, filename, driver_name, encoding):
        self.filename = filename
        self.encoding = encoding
        driver = ogr.GetDriverByName(driver_name)
        self.source = driver.Open(filename, 0)
        self._num_features = None
        self._layers = None

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

    def get_records(self, start, num):
        layer_idx, layer_start = get_start_layer(self.layers, start)
        c = 0
        for i, layer in enumerate(self.layers[layer_idx:]):
            if i > 0:
                layer_start = 0
            gen = layer.get_records(layer_start, num)
            for r in gen:
                yield r
                c += 1
                if c > num:
                    break

    def __del__(self):
        if self.source is not None:
            self.source.Destroy()
            self.source = None
