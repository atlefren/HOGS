# -*- coding: utf-8 -*-

from OgrFeature import OgrFeature
from importer.helpers import normalize


def get_pg_type(ogr_type):
    types = {
        0: 'integer',
        1: 'integer[]',
        2: 'real',
        3: 'real[]',
        4: 'text',
        5: 'text[]',
        6: 'text',
        7: 'text[]',
        8: 'bytea',
        9: 'date',
        10: 'time',
        11: 'timestamp',
        12: 'bigint',
        13: 'bigint[]',
    }
    return types[ogr_type]


class OgrLayer(object):
    def __init__(self, ogr_layer, encoding):
        self.ogr_layer = ogr_layer
        self._schema = None
        self.encoding = encoding
        self._count = None

    @property
    def name(self):
        return self.ogr_layer.GetName()

    @property
    def num_features(self):
        if self._count is None:
            self._count = self.ogr_layer.GetFeatureCount()
            self.ogr_layer.ResetReading()
        return self._count

    def features(self):
        for feature in self.ogr_layer:
            yield OgrFeature(feature, self.schema, self.encoding)

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
                    'type': field_type,
                    'pg_type': get_pg_type(field_type)
                })
        return self._schema
