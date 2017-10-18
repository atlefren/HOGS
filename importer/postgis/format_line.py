# -*- coding: utf-8 -*-
import json

from importer.helpers import escape, DateTimeEncoder


def get_line_formatter(columns):

    def format_line(record):
        line_template = '\t'.join(['%s'] * len(columns))
        data = []
        for column in columns:
            value = None
            if column in record:
                value = record[column]
            else:
                value = record['attribs'].get(column, None)
            if value is None or value == '':
                data.append('\N')
            elif isinstance(value, dict):
                data.append(escape(json.dumps(
                    value,
                    ensure_ascii=False,
                    cls=DateTimeEncoder
                )))
            else:
                data.append(value)
        return unicode(line_template % tuple(data))
    return format_line
