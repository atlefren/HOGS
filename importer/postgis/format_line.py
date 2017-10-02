# -*- coding: utf-8 -*-
import json

from importer.helpers import escape, DateTimeEncoder


def format_line(record):
    geom = '\N'
    attributes = '\N'
    is_valid = 'true' if record['valid'] else 'false'
    invalid_reason = '\N'

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
        record.get('version', '\N'),
        geom,
        attributes,
        is_valid,
        invalid_reason,
        record.get('filename', '\N')
    )
    return unicode(line)
