# -*- coding: utf-8 -*-

from ctypes.util import find_library
from ctypes import (CDLL, DEFAULT_MODE, POINTER,
                    c_void_p, c_char_p,
                    c_int, c_longlong, c_ubyte, c_double, c_int64,
                    c_size_t, pointer,
                    string_at, byref)


c_int_p = POINTER(c_int)  # shortcut type


class allocated_c_char_p(c_char_p):
    pass

_lgdal = CDLL('libgdal.so')

_lgdal.VSIFree.restype = None
_lgdal.VSIFree.argtypes = [c_void_p]

_lgdal.GDALAllRegister.restype = None
_lgdal.GDALAllRegister.argtypes = []

# driver
_lgdal.OGRGetDriverByName.restype = c_void_p
_lgdal.OGRGetDriverByName.argtypes = [allocated_c_char_p]

_lgdal.OGR_Dr_Open.restype = c_void_p
_lgdal.OGR_Dr_Open.argtypes = [c_void_p, allocated_c_char_p, c_int]

# Datasource
_lgdal.OGR_DS_Destroy.restype = None
_lgdal.OGR_DS_Destroy.argtypes = [c_void_p]

_lgdal.OGR_DS_GetLayerCount.restype = c_int
_lgdal.OGR_DS_GetLayerCount.argtypes = [c_void_p]

_lgdal.OGR_DS_GetLayer.restype = c_void_p
_lgdal.OGR_DS_GetLayer.argtypes = [c_void_p, c_int]

# Layer
_lgdal.OGR_L_GetFeatureCount.restype = c_longlong
_lgdal.OGR_L_GetFeatureCount.argtypes = [c_void_p, c_int]

_lgdal.OGR_L_GetNextFeature.restype = c_void_p
_lgdal.OGR_L_GetNextFeature.argtypes = [c_void_p]

_lgdal.OGR_L_GetName.restype = c_char_p
_lgdal.OGR_L_GetName.argtypes = [c_void_p]

_lgdal.OGR_L_GetSpatialRef.restype = c_void_p
_lgdal.OGR_L_GetSpatialRef.argtypes = [c_void_p]

_lgdal.OGR_L_GetLayerDefn.restype = c_void_p
_lgdal.OGR_L_GetLayerDefn.argtypes = [c_void_p]

# Feature
_lgdal.OGR_F_GetGeometryRef.restype = c_void_p
_lgdal.OGR_F_GetGeometryRef.argtypes = [c_void_p]

_lgdal.OGR_F_Destroy.restype = None
_lgdal.OGR_F_Destroy.argtypes = [c_void_p]

_lgdal.OGR_F_GetRawFieldRef.restype = c_void_p
_lgdal.OGR_F_GetRawFieldRef.argtypes = [c_void_p, c_int]

# Geometry
_lgdal.OGR_G_IsValid.restype = c_int
_lgdal.OGR_G_IsValid.argtypes = [c_void_p]

_lgdal.OGR_G_DestroyGeometry.restype = None
_lgdal.OGR_G_DestroyGeometry.argtypes = [c_void_p]

_lgdal.OGR_G_ExportToWkt.restype = c_void_p
_lgdal.OGR_G_ExportToWkt.argtypes = [c_void_p]

_lgdal.OGR_G_WkbSize.restype = c_int
_lgdal.OGR_G_WkbSize.argtypes = [c_void_p]

_lgdal.OGR_G_ExportToWkt.restype = c_int
_lgdal.OGR_G_ExportToWkt.argtypes = [c_void_p, POINTER(c_char_p)]

_lgdal.OGR_G_ExportToWkb.restype = c_int
_lgdal.OGR_G_ExportToWkb.argtypes = None

_lgdal.OGR_G_TransformTo.restype = c_int
_lgdal.OGR_G_TransformTo.argtypes = [c_void_p, c_void_p]


# field definition
_lgdal.OGR_FD_GetFieldCount.restype = c_int
_lgdal.OGR_FD_GetFieldCount.argtypes = [c_void_p]

_lgdal.OGR_FD_GetFieldDefn.restype = c_void_p
_lgdal.OGR_FD_GetFieldDefn.argtypes = [c_void_p, c_int]

_lgdal.OGR_Fld_GetNameRef.restype = c_char_p
_lgdal.OGR_Fld_GetNameRef.argtypes = [c_void_p]

_lgdal.OGR_Fld_GetType.restype = c_int
_lgdal.OGR_Fld_GetType.argtypes = [c_void_p]

# field
_lgdal.OGR_F_IsFieldNull.restype = c_int
_lgdal.OGR_F_IsFieldNull.argtypes = [c_void_p, c_int]


_lgdal.OGR_F_GetFieldAsString.restype = c_char_p
_lgdal.OGR_F_GetFieldAsString.argtypes = [c_void_p, c_int]

_lgdal.OGR_F_GetFieldAsInteger.restype = c_int
_lgdal.OGR_F_GetFieldAsInteger.argtypes = [c_void_p, c_int]

_lgdal.OGR_F_GetFieldAsInteger64.restype = c_int64
_lgdal.OGR_F_GetFieldAsInteger64.argtypes = [c_void_p, c_int]

_lgdal.OGR_F_GetFieldAsDouble.restype = c_double
_lgdal.OGR_F_GetFieldAsDouble.argtypes = [c_void_p, c_int]

_lgdal.OGR_F_GetFieldAsDateTime.restype = c_int
_lgdal.OGR_F_GetFieldAsDateTime.argtypes = [c_void_p, c_int, c_int_p, c_int_p, c_int_p, c_int_p, c_int_p, c_int_p]

# Spatial ref
_lgdal.OSRNewSpatialReference.restype = c_void_p
_lgdal.OSRNewSpatialReference.argtypes = [c_char_p]

_lgdal.OSRImportFromEPSG.restype = c_int
_lgdal.OSRImportFromEPSG.argtypes = [c_void_p, c_int]

_lgdal.GDALAllRegister()

lgdal = _lgdal