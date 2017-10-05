# -*- coding: utf-8 -*-
from datetime import date, datetime, time


class MissingDatatypeException(Exception):
    pass


def get_int(ogr_feature, field_idx, encoding):
    return ogr_feature.GetFieldAsInteger(field_idx)


def get_int_list(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('int list')
    return None


def get_real(ogr_feature, field_idx, encoding):
    return ogr_feature.GetFieldAsDouble(field_idx)


def get_real_list(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('real list')
    return None


def get_string(ogr_feature, field_idx, encoding):
    value = ogr_feature.GetFieldAsString(field_idx)
    s = unicode(value.decode(encoding))
    if s == u'':
        return None
    return s


def get_string_list(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('string list')
    return None


def get_wide_string(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('wide string')
    return None


def get_wide_string_list(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('wide string list')
    return None


def get_binary(ogr_feature, field_idx, encoding):
    return ogr_feature.GetFieldAsBinary(field_idx)


def field_as_datetime(ogr_feature, field_idx):
    res = ogr_feature.GetFieldAsDateTime(field_idx)
    if not res:
        return None
    return (res[0], res[1], res[2], res[3], res[4], int(res[5]))


def get_date(ogr_feature, field_idx, encoding):
    res = field_as_datetime(ogr_feature, field_idx)
    if res is None:
        return None
    (yy, mm, dd, hh, mn, ss) = res
    try:
        return date(yy, mm, dd)
    except ValueError:
        return None


def get_time(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('time')
    return None


def get_datetime(ogr_feature, field_idx, encoding):
    res = field_as_datetime(ogr_feature, field_idx)
    if res is None:
        return None
    (yy, mm, dd, hh, mn, ss) = res
    try:
        return datetime(yy, mm, dd, hh, mn, ss)
    except ValueError:
        return None


def get_int64(ogr_feature, field_idx, encoding):
    return ogr_feature.GetFieldAsInteger64(field_idx)


def get_int64_list(ogr_feature, field_idx, encoding):
    raise MissingDatatypeException('int64 list')
    return None


def get_field_value(ogr_feature, field_idx, field_type, encoding):
    types = {
        0: get_int,
        1: get_int_list,
        2: get_real,
        3: get_real_list,
        4: get_string,
        5: get_string_list,
        6: get_wide_string,
        7: get_wide_string_list,
        8: get_binary,
        9: get_date,
        10: get_time,
        11: get_datetime,
        12: get_int64,
        13: get_int64_list
    }
    return types[field_type](ogr_feature, field_idx, encoding)
