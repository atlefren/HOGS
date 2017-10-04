# -*- coding: utf-8 -*-

from osgeo import ogr
from OgrLayer import OgrLayer


class OgrFile(object):

    def __init__(self, filename, driver_name, encoding):
        self.filename = filename
        self.encoding = encoding
        driver = ogr.GetDriverByName(driver_name)
        self.source = driver.Open(filename, 0)

    def layers(self):
        num_layers = self.source.GetLayerCount()
        for i in range(0, num_layers):
            yield OgrLayer(self.source.GetLayerByIndex(i), self.encoding)

    @property
    def num_features(self):
        return sum([layer.num_features for layer in self.layers()])

    def __del__(self):
        if self.source is not None:
            self.source.Destroy()
            self.source = None
