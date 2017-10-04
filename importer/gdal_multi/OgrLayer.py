# -*- coding: utf-8 -*-

#from lgdal import lgdal
from OgrFeature import OgrFeature
from importer.helpers import normalize


class OgrLayer(object):
    def __init__(self, ogr_layer, encoding):
        self.ogr_layer = ogr_layer
        self._schema = None
        self.encoding = encoding

    @property
    def name(self):
        pass
        #return string_at(lgdal.OGR_L_GetName(self.ogr_layer))

    @property
    def num_features(self):
        return self.ogr_layer.GetFeatureCount()

    def features(self):
        for feature in self.ogr_layer:
            yield OgrFeature(feature, self.schema, self.encoding)

    def get_records(self, start, num):
        self.ogr_layer.SetNextByIndex(start)
        c = 0
        while c < num:
            f = next(self.ogr_layer)
            f = OgrFeature(f, self.schema, self.encoding)
            yield f
            c += 1

    @property
    def schema(self):
        if self._schema is None:

            layer_def = self.ogr_layer.GetLayerDefn()
            self._schema = []
            for index in range(layer_def.GetFieldCount()):
                field_def = layer_def.GetFieldDefn(index)
                field_name = field_def.GetName()
                field_type = field_def.GetType()

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
