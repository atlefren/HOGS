# -*- coding: utf-8 -*-

import codecs
import os

from osgeo import ogr, osr
import magic

from helpers import create_table_name, to_ewkb_hex, Geometry


def get_encoding(filename):
    blob = open(filename).read()
    m = magic.Magic(mime_encoding=True)
    encoding = m.from_buffer(blob)
    return encoding


def convert_file(filename):
    name, file_extension = os.path.splitext(filename)
    base = os.path.basename(filename)
    if file_extension.lower() != '.sos':
        return filename
    if get_encoding(filename) != 'utf-8':
        return filename

    isopath = os.path.join(os.path.dirname(filename), 'iso88591')
    if not os.path.exists(isopath):
        os.makedirs(isopath)
    new_filename = os.path.join(isopath, base)
    with open(filename, 'r') as infile:
        with open(new_filename, 'wb') as outfile:
            outfile.write(unicode(infile.read(), 'utf-8-sig').encode('latin-1', 'replace'))
            return new_filename


def get_driver(filename):
    _, file_extension = os.path.splitext(filename)
    if file_extension.lower() == '.sos':
        return ogr.GetDriverByName('SOSI')
    if file_extension.lower() == '.geojson':
        return ogr.GetDriverByName('GeoJSON')
    return None


def get_type(field_def):

    types = {
        0: 'int',
        1: 'int[]',
        2: 'float',
        3: 'float[]',
        4: 'string',
        5: 'string[]',
        8: 'binary',
        9: 'date',
        10: 'time',
        11: 'datetime',
        12: 'int64',
        13: 'int64[]'
    }
    type_enum = field_def.GetType()
    if type_enum in types:
        return types[type_enum]
    return 'unknown'


def get_fields_from_layer(layer, encoding):
        m = magic.Magic(mime_encoding=True)
        layer_defn = layer.GetLayerDefn()
        fields = []
        for i in range(0, layer_defn.GetFieldCount()):
            field_def = layer_defn.GetFieldDefn(i)
            field_name = None
            try:
                field_name = unicode(field_def.GetName().decode('utf-8'))
            except UnicodeDecodeError:
                field_name = unicode(field_def.GetName().decode(encoding))
            if field_name is not None:
                fields.append({
                    'name': field_name,
                    'type': get_type(field_def),
                    'normalized': create_table_name(field_name)
                })
        return fields


def uniq(list_of_dicts, key):
    seen_values = set()
    without_duplicates = []
    for d in list_of_dicts:
        value = d[key]
        if value not in seen_values:
            without_duplicates.append(d)
            seen_values.add(value)
    return without_duplicates


class OgrFile(object):

    def __init__(self, filename):
        self.filename = convert_file(filename)
        self.encoding = get_encoding(self.filename)
        driver = get_driver(self.filename)
        self.source = driver.Open(self.filename, 0)

    def fields(self):
        fields = []
        for i in range(self.source.GetLayerCount()):
            layer = self.source.GetLayerByIndex(i)
            layer_defn = layer.GetLayerDefn()
            fields += get_fields_from_layer(layer, self.encoding)
        f = uniq(fields, 'name')
        return f

    def records(self):
        for i in range(self.source.GetLayerCount()):
            layer = self.source.GetLayerByIndex(i)
            ogr_layer = OgrFileLayer(layer, self.encoding)
            for record in ogr_layer.records():
                yield record

    def destroy(self):
        self.source.Destroy()


class OgrFileLayer(object):

    def __init__(self, layer, encoding):

        self.layer = layer
        self.encoding = encoding

        out_srs = osr.SpatialReference()
        out_srs.ImportFromEPSG(4326)

        in_srs = self.layer.GetSpatialRef()

        self.coord_trans = osr.CoordinateTransformation(in_srs, out_srs)

        self.num_features = self.layer.GetFeatureCount()
        self._fields = None

    def fields(self):
        if self._fields is None:
            self._fields = get_fields_from_layer(self.layer, self.encoding)
        return self._fields

    def records(self):
        fields = self.fields()
        for feature in self.layer:
            geom = feature.GetGeometryRef()
            geom.Transform(self.coord_trans)
            attrs = {}
            for i, field in enumerate(fields):
                value = feature.GetField(i)

                if isinstance(value, basestring):
                    value = unicode(value.decode(self.encoding))
                attrs[field['normalized']] = value
            g = Geometry(geom)
            if g.is_valid():
                yield {'geom': g.ewkb_hex(), 'properties': attrs}
            else:
                reason = g.is_valid_reason()
                print 'invalid:', reason
                print 'geom:', g.wkt()
                print '---'
                yield {'geom': None, 'properties': attrs, 'reason': reason}
