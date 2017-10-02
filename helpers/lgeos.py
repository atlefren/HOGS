from ctypes import (CDLL, DEFAULT_MODE, c_char_p, c_void_p, c_int, c_size_t,
                    POINTER, pointer, string_at, CFUNCTYPE)
from ctypes.util import find_library


c_size_t_p = POINTER(c_size_t)


class allocated_c_char_p(c_char_p):
    pass


def error_handler(fmt, *fmt_args):
    pass


def notice_handler(fmt, *fmt_args):
    pass


_lgeos = CDLL(find_library('geos_c'), mode=DEFAULT_MODE)

EXCEPTION_HANDLER_FUNCTYPE = CFUNCTYPE(None, c_char_p, c_void_p)
_lgeos.initGEOS.restype = None
_lgeos.initGEOS.argtypes = [EXCEPTION_HANDLER_FUNCTYPE, EXCEPTION_HANDLER_FUNCTYPE]


_lgeos.GEOSWKBWriter_create.restype = c_void_p
_lgeos.GEOSWKBWriter_create.argtypes = []

_lgeos.GEOSWKBWriter_destroy.restype = None
_lgeos.GEOSWKBWriter_destroy.argtypes = [c_void_p]

_lgeos.GEOSWKBWriter_writeHEX.restype = allocated_c_char_p
_lgeos.GEOSWKBWriter_writeHEX.argtypes = [c_void_p, c_void_p, c_size_t_p]

_lgeos.GEOSWKBWriter_setIncludeSRID.restype = None
_lgeos.GEOSWKBWriter_setIncludeSRID.argtypes = [c_void_p, c_int]


_lgeos.GEOSWKBReader_create.restype = c_void_p
_lgeos.GEOSWKBReader_create.argtypes = []

_lgeos.GEOSWKBReader_read.restype = c_void_p
_lgeos.GEOSWKBReader_read.argtypes = [c_void_p, c_char_p, c_size_t]

_lgeos.GEOSSetSRID.restype = None
_lgeos.GEOSSetSRID.argtypes = [c_void_p, c_int]

_lgeos.GEOSFree.restype = None
_lgeos.GEOSFree.argtypes = [c_void_p]

_lgeos.GEOSisValidReason.restype = allocated_c_char_p
_lgeos.GEOSisValidReason.argtypes = [c_void_p]


error_h = EXCEPTION_HANDLER_FUNCTYPE(error_handler)
notice_h = EXCEPTION_HANDLER_FUNCTYPE(notice_handler)


geos_handle = _lgeos.initGEOS(notice_h, error_h)

lgeos = _lgeos
