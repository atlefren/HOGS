# -*- coding: utf-8 -*-
import json

from importer.helpers import escape, DateTimeEncoder


def format_line(record):
    geom = '\N'
    attributes = '\N'
    is_valid = 'true' if record['valid'] else 'false'
    invalid_reason = '\N'

    filename = record.get('filename', '\N')
    if filename == '':
        filename = '\N'

    if record['valid']:
        geom = record['geom']
        attributes = escape(json.dumps(
            record['attributes'],
            ensure_ascii=False,
            cls=DateTimeEncoder
        ))
    else:
        invalid_reason = record['reason']


    line = '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
        record.get('dataset_id', '\N'),
        record.get('version', '10000'),
        geom,
        attributes,
        filename,
        is_valid,
        invalid_reason
    )
    return unicode(line)
