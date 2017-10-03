# -*- coding: utf-8 -*-

from ctypes import c_int, byref
from datetime import date, datetime, time
from lgdal import lgdal


def get_int(ogr_feature, field_idx, encoding):
    return lgdal.OGR_F_GetFieldAsInteger(ogr_feature, field_idx)


def get_int_list(ogr_feature, field_idx, encoding):
    return None


def get_real(ogr_feature, field_idx, encoding):
    return None


def get_real_list(ogr_feature, field_idx, encoding):
    return None


def get_string(ogr_feature, field_idx, encoding):
    value = lgdal.OGR_F_GetFieldAsString(ogr_feature, field_idx)
    return unicode(value.decode(encoding))


def get_string_list(ogr_feature, field_idx, encoding):
    return None


def get_wide_string(ogr_feature, field_idx, encoding):
    return None


def get_wide_string_list(ogr_feature, field_idx, encoding):
    return None


def get_binary(ogr_feature, field_idx, encoding):
    return None


def field_as_datetime(ogr_feature, field_idx):
    yy, mm, dd, hh, mn, ss, tz = [c_int() for i in range(7)]
    status = lgdal.OGR_F_GetFieldAsDateTime(ogr_feature, field_idx, byref(yy), byref(mm), byref(dd), byref(hh), byref(mn), byref(ss), byref(tz))
    if not status:
        return None
    return (yy.value, mm.value, dd.value, hh.value, mn.value, ss.value)


def get_date(ogr_feature, field_idx, encoding):
    res = field_as_datetime(ogr_feature, field_idx)
    if res is None:
        return None
    (yy, mm, dd, hh, mn, ss) = res
    return date(yy, mm, dd)


def get_time(ogr_feature, field_idx, encoding):
    return None


def get_datetime(ogr_feature, field_idx, encoding):
    res = field_as_datetime(ogr_feature, field_idx)
    if res is None:
        return None
    (yy, mm, dd, hh, mn, ss) = res
    return datetime(yy, mm, dd, hh, mn, ss)


def get_int64(ogr_feature, field_idx, encoding):
    return None


def get_int64_list(ogr_feature, field_idx, encoding):
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

