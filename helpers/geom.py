from osgeo import gdal
from ctypes import (c_char_p, c_size_t, pointer, string_at)

from lgeos import lgeos

gdal.PushErrorHandler('CPLQuietErrorHandler')


def ogr_to_geos_geom(ogr_geom):
    wkb = ogr_geom.ExportToWkb()
    reader = lgeos.GEOSWKBReader_create()
    return lgeos.GEOSWKBReader_read(reader, c_char_p(wkb), c_size_t(len(wkb)))


class Geometry(object):

    def __init__(self, ogr_geom):
        self._ogr_geom = ogr_geom
        self._is_valid = ogr_geom is not None and ogr_geom.IsValid()
        self._geos_geom = None

    def _get_geos_geom(self):
        if self._geos_geom is None:
            self._geos_geom = ogr_to_geos_geom(self._ogr_geom)
        return self._geos_geom

    def is_valid(self):
        return self._is_valid

    def transform(self, coord_trans):
        self._ogr_geom.Transform(coord_trans)

    def is_valid_reason(self):
        if self._is_valid:
            return None
        geos_geom = self._get_geos_geom()
        if geos_geom is None:
            return 'No geometry'

        result = lgeos.GEOSisValidReason(geos_geom)
        text = string_at(result)
        lgeos.GEOSFree(result)
        return text

    def wkt(self):
        return self._ogr_geom.ExportToWkt()

    def ewkb_hex(self, srid=4326):
        geos_geom = self._get_geos_geom()
        if geos_geom is None:
            return None
        writer = lgeos.GEOSWKBWriter_create()
        lgeos.GEOSWKBWriter_setIncludeSRID(writer, bool(True))
        lgeos.GEOSSetSRID(geos_geom, srid)
        size = c_size_t()
        result = lgeos.GEOSWKBWriter_writeHEX(writer, geos_geom, pointer(size))
        data = string_at(result, size.value)

        lgeos.GEOSFree(result)
        return data
