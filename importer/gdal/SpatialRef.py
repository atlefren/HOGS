# -*- coding: utf-8 -*-

from osgeo import osr


class SpatialRef(object):

    def __init__(self, srid):
        self.srid = srid
        self.srs = osr.SpatialReference()
        self.srs.ImportFromEPSG(self.srid)
