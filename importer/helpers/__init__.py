# -*- coding: utf-8 -*-
import re
import unicodedata
from datetime import datetime, date
import json
from paralell import paralellize


def create_table_name(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = value.lower()
    value = value.replace(u'å', 'aa')
    value = value.replace(u'æ', 'ae')
    value = value.replace(u'ø', 'o')
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip())
    value = unicode(re.sub('[-\s]+', '_', value))
    return value


def normalize(value):
    value = value.lower()
    value = value.replace(u'å', 'aa')
    value = value.replace(u'æ', 'ae')
    value = value.replace(u'ø', 'o')
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = re.sub('[^\w\s-]', '', value).strip()
    value = re.sub('[-\s]+', '_', value)
    return value


def escape(value):
    return value.replace('\\', '\\\\')


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)
