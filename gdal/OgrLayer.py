# -*- coding: utf-8 -*-

from lgdal import lgdal
from OgrFeature import OgrFeature
from helpers import normalize


class OgrLayer(object):
    def __init__(self, ogr_layer, encoding):
        self.ogr_layer = ogr_layer
        self._schema = None
        self.encoding = encoding

    @property
    def name(self):
        return string_at(lgdal.OGR_L_GetName(self.ogr_layer))

    def features(self):
        while True:
            feature = lgdal.OGR_L_GetNextFeature(self.ogr_layer)
            if feature is not None:
                yield OgrFeature(feature, self.schema, self.encoding)
            else:
                break

    @property
    def schema(self):
        if self._schema is None:

            layer_def = lgdal.OGR_L_GetLayerDefn(self.ogr_layer)
            field_count = lgdal.OGR_FD_GetFieldCount(layer_def)
            self._schema = []
            for index in range(0, field_count):
                field_def = lgdal.OGR_FD_GetFieldDefn(layer_def, index)
                field_name = lgdal.OGR_Fld_GetNameRef(field_def)
                field_type = lgdal.OGR_Fld_GetType(field_def)

                try:
                    field_name = unicode(field_name.decode('utf-8'))
                except UnicodeDecodeError:
                    field_name = unicode(field_name.decode(self.encoding))

                self._schema.append({
                    'name': field_name,
                    'normalized': normalize(field_name),
                    'index': index,
                    'type': field_type
                })
        return self._schema
