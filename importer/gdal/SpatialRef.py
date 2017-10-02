# -*- coding: utf-8 -*-

from ctypes import c_char_p
from lgdal import lgdal


class SpatialRef(object):

    def __init__(self, srid):
        self.srid = srid
        buf = c_char_p(b'')
        self.srs = lgdal.OSRNewSpatialReference(buf)
        lgdal.OSRImportFromEPSG(self.srs, self.srid)
