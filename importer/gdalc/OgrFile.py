# -*- coding: utf-8 -*-

from lgdal import lgdal
from OgrLayer import OgrLayer


class OgrFile(object):

    def __init__(self, filename, driver_name, encoding):
        self.filename = filename
        self.encoding = encoding
        driver = lgdal.OGRGetDriverByName(driver_name)
        self.source = lgdal.OGR_Dr_Open(driver, self.filename, bool(False))

    def layers(self):
        num_layers = lgdal.OGR_DS_GetLayerCount(self.source)
        for i in range(0, num_layers):
            yield OgrLayer(lgdal.OGR_DS_GetLayer(self.source, i), self.encoding)

    def __del__(self):
        if self.source is not None and lgdal is not None:
            lgdal.OGR_DS_Destroy(self.source)
            self.source = None
