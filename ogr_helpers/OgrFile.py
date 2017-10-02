# -*- coding: utf-8 -*-
import codecs
import os

import magic

from helpers import create_table_name, to_ewkb_hex, Geometry
from gdal import OgrFile as NativeOgrFile, SpatialRef


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
        return 'SOSI'
    if file_extension.lower() == '.geojson':
        return 'GeoJSON'
    return None


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
        self.org_filename = filename
        self.filename = convert_file(filename)
        self.encoding = get_encoding(self.filename)
        driver_name = get_driver(self.filename)
        self.file = NativeOgrFile(self.filename, driver_name, self.encoding)
        self.out_srs = SpatialRef(4326)

    def fields(self):
        fields = []
        for layer in self.file.layers():
            fields += layer.schema
        return uniq(fields, 'name')

    def records(self):
        for layer in self.file.layers():
            for feature in layer.features():
                if not feature.geom_valid:
                    yield {
                        'valid': False,
                        'geom': feature.wkt,
                        'reason': feature.invalid_reason
                    }
                else:
                    feature.transform(self.out_srs)
                    yield {
                        'valid': True,
                        'geom': feature.ewkb_hex,
                        'attributes': feature.attributes
                    }
