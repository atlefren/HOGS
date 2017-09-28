from ctypes import CDLL, DEFAULT_MODE, c_char_p, c_void_p, c_int, c_size_t, POINTER, pointer, string_at, CFUNCTYPE
from ctypes.util import find_library

c_size_t_p = POINTER(c_size_t)


class allocated_c_char_p(c_char_p):
    pass


lgeos = CDLL(find_library('geos_c'), mode=DEFAULT_MODE)

EXCEPTION_HANDLER_FUNCTYPE = CFUNCTYPE(None, c_char_p, c_void_p)
lgeos.initGEOS.restype = None
lgeos.initGEOS.argtypes = [EXCEPTION_HANDLER_FUNCTYPE, EXCEPTION_HANDLER_FUNCTYPE]


lgeos.GEOSFree.restype = None
lgeos.GEOSFree.argtypes = [c_void_p]


lgeos.GEOSWKBWriter_create.restype = c_void_p
lgeos.GEOSWKBWriter_create.argtypes = []

lgeos.GEOSWKBWriter_writeHEX.restype = allocated_c_char_p
lgeos.GEOSWKBWriter_writeHEX.argtypes = [c_void_p, c_void_p, c_size_t_p]

lgeos.GEOSWKTReader_create.restype = c_void_p
lgeos.GEOSWKTReader_create.argtypes = []

lgeos.GEOSWKTReader_read.restype = c_void_p
lgeos.GEOSWKTReader_read.argtypes = [c_void_p, c_char_p, c_size_t]


lgeos.GEOSWKTWriter_create.restype = c_void_p
lgeos.GEOSWKTWriter_create.argtypes = []

lgeos.GEOSWKTWriter_write.restype = allocated_c_char_p
lgeos.GEOSWKTWriter_write.argtypes = [c_void_p, c_void_p]


def error_handler(fmt, *fmt_args):
    pass


def notice_handler(fmt, *fmt_args):
    pass

error_h = EXCEPTION_HANDLER_FUNCTYPE(error_handler)
notice_h = EXCEPTION_HANDLER_FUNCTYPE(notice_handler)

geos_handle = lgeos.initGEOS(notice_h, error_h)


data = 'POINT(1 2)'
reader = lgeos.GEOSWKBReader_create()
geom = lgeos.GEOSWKTReader_read(reader, c_char_p(data), c_size_t(len(data)))

print geom

writer = lgeos.GEOSWKBWriter_create()

size = c_size_t()
result = lgeos.GEOSWKBWriter_writeHEX(writer, geom, pointer(size))
text = string_at(result)
lgeos.GEOSFree(result)

print text
