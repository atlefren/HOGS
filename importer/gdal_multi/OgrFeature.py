# -*- coding: utf-8 -*-
from ctypes import c_ubyte, byref, string_at, c_char_p, c_size_t, pointer
import io

from importer.geos import lgeos

from get_field_value import get_field_value
from stdout_redirector import stdout_redirector


class OgrFeature(object):

    def __init__(self, ogr_feature, schema, encoding):
        self.ogr_feature = ogr_feature
        self.ogr_geom = ogr_feature.GetGeometryRef()
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
            valid = self.ogr_geom.IsValid()
        self._valid = valid
        self._err = f.getvalue()
        return self._valid

    @property
    def invalid_reason(self):
        if self.geom_valid:
            return None
        return self._err

    @property
    def wkt(self):
        return self.ogr_geom.ExportToWkt()

    @property
    def wkb(self):
        return self.ogr_geom.ExportToWkb()

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
            attrs[field['name']] = value
        return attrs

    def transform(self, srs):
        self.ogr_geom.TransformTo(srs.srs)
        self._srid = srs.srid

    def to_db(self, out_srs):
        if not self.geom_valid:
            return {
                'valid': False,
                'geom': self.wkt,
                'reason': self.invalid_reason
            }
        else:
            self.transform(out_srs)
            return {
                'valid': True,
                'geom': self.ewkb_hex,
                'attributes': self.attributes
            }

    def __del__(self):
        if self.ogr_feature:
            self.ogr_feature.Destroy()
            self.ogr_feature = None