from osgeo import gdal
from ctypes import (c_char_p, c_size_t, pointer, string_at)

from lgeos import lgeos

gdal.PushErrorHandler('CPLQuietErrorHandler')


def ogr_to_geos_geom(ogr_geom):
    wkb = ogr_geom.ExportToWkb()
    reader = lgeos.GEOSWKBReader_create()
    g = lgeos.GEOSWKBReader_read(reader, c_char_p(wkb), c_size_t(len(wkb)))
    if g is None:
        print ogr_geom
    return g


class Geometry(object):

    def __init__(self, ogr_geom, srid=4326):
        self._wkt = None
        if ogr_geom is not None:
            self.geos_geom = ogr_to_geos_geom(ogr_geom)
            if self.geos_geom is not None:
                lgeos.GEOSSetSRID(self.geos_geom, srid)
                self._is_valid = ogr_geom.IsValid()
            else:
                self._is_valid = False
                self._wkt = ogr_geom.ExportToWkt()
        else:
            self.geos_geom = None
            self._is_valid = False

    def is_valid(self):
        return self._is_valid

    def is_valid_reason(self):
        if self._is_valid:
            return None
        if self.geos_geom is None:
            return 'No geometry'

        result = lgeos.GEOSisValidReason(self.geos_geom)
        text = string_at(result)
        lgeos.GEOSFree(result)
        return text

    def wkt(self):
        return self._wkt

    def ewkb_hex(self):
        if self.geos_geom is None:
            return None
        writer = lgeos.GEOSWKBWriter_create()
        lgeos.GEOSWKBWriter_setIncludeSRID(writer, bool(True))

        size = c_size_t()
        result = lgeos.GEOSWKBWriter_writeHEX(writer, self.geos_geom, pointer(size))
        data = string_at(result, size.value)

        lgeos.GEOSFree(result)
        return data
