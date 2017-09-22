# -*- coding: utf-8 -*-

import codecs
import os

import ogr
import osr
import magic

from helpers import normalize


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


class OgrFile(object):

    def __init__(self, filename):

        filename = convert_file(filename)
        driver = get_driver(filename)
        self.source = driver.Open(filename, 0)
        self.layer = self.source.GetLayer()

        self.encoding = get_encoding(filename)

        out_srs = osr.SpatialReference()
        out_srs.ImportFromEPSG(4326)

        in_srs = self.layer.GetSpatialRef()

        self.coord_trans = osr.CoordinateTransformation(in_srs, out_srs)

        self.num_features = self.layer.GetFeatureCount()
        self._fields = None

    def _get_fields_from_layer(self):
        m = magic.Magic(mime_encoding=True)
        layer_defn = self.layer.GetLayerDefn()
        fields = []
        for i in range(0, layer_defn.GetFieldCount()):
            field_def = layer_defn.GetFieldDefn(i)
            field_name = unicode(field_def.GetName().decode('utf-8'))
            fields.append({
                'name': field_name,
                'normalized': create_table_name(field_name)
            })
        return fields

    def fields(self):
        if self._fields is None:
            self._fields = self._get_fields_from_layer()
        return self._fields

    def records(self):
        fields = self.fields()
        for feature in self.layer:
            geom = feature.GetGeometryRef()
            geom.Transform(self.coord_trans)
            geom_wkt = geom.ExportToWkt()
            attrs = {}
            for i, field in enumerate(fields):
                value = feature.GetField(i)

                if isinstance(value, basestring):
                    value = unicode(value.decode(self.encoding))
                attrs[field['normalized']] = value

            yield {'geom': geom_wkt, 'properties': attrs}

    def destroy(self):
        self.source.Destroy()

