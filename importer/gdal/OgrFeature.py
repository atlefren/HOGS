# -*- coding: utf-8 -*-

from ctypes import c_ubyte, byref, string_at, c_char_p, c_size_t, pointer
import io

from lgdal import lgdal
from importer.geos import lgeos

from get_field_value import get_field_value
from stdout_redirector import stdout_redirector


def wkb_size(ogr_geom):
    return lgdal.OGR_G_WkbSize(ogr_geom)


class OgrFeature(object):

    def __init__(self, ogr_feature, schema, encoding):
        self.ogr_feature = ogr_feature
        self.ogr_geom = lgdal.OGR_F_GetGeometryRef(ogr_feature)
        self._schema = schema
        self.encoding = encoding
        self._valid = None
        self._err = None
        self._srid = None

    @property
    def geom_valid(self):
        if self._valid is not None:
            return self._valid

        valid = None
        f = io.StringIO()
        with stdout_redirector(f):
            valid = bool(lgdal.OGR_G_IsValid(self.ogr_geom))
        self._valid = valid
        self._err = f.getvalue()
        #print self._err
        return self._valid

    @property
    def invalid_reason(self):
        if self.geom_valid:
            return None
        return self._err

    @property
    def wkt(self):
        p = c_char_p()
        res = lgdal.OGR_G_ExportToWkt(self.ogr_geom, byref(p))
        wkt = p.value
        lgdal.VSIFree(p)
        return wkt

    @property
    def wkb(self):
        size = wkb_size(self.ogr_geom)
        buf = (c_ubyte * size)()
        res = lgdal.OGR_G_ExportToWkb(self.ogr_geom, 0, byref(buf))
        val = string_at(buf, size)
        return val

    @property
    def ewkb_hex(self):
        if self._srid is None:
            return None

        wkb = self.wkb
        reader = lgeos.GEOSWKBReader_create()
        geos_geom = lgeos.GEOSWKBReader_read(reader, c_char_p(wkb), c_size_t(len(wkb)))
        writer = lgeos.GEOSWKBWriter_create()
        lgeos.GEOSWKBWriter_setIncludeSRID(writer, bool(True))
        lgeos.GEOSSetSRID(geos_geom, self._srid)
        size = c_size_t()
        result = lgeos.GEOSWKBWriter_writeHEX(writer, geos_geom, pointer(size))
        data = string_at(result, size.value)
        lgeos.GEOSFree(result)
        return data

    @property
    def attributes(self):
        attrs = {}
        for field in self._schema:
            value = get_field_value(self.ogr_feature, field['index'], field['type'], self.encoding)
            #if value is None:
            #    print field
            attrs[field['name']] = value
        return attrs

    def transform(self, srs):
        lgdal.OGR_G_TransformTo(self.ogr_geom, srs.srs)
        self._srid = srs.srid

    def __del__(self):
        if self.ogr_feature is not None and lgdal is not None:
            lgdal.OGR_F_Destroy(self.ogr_feature)
            self.ogr_feature = None
