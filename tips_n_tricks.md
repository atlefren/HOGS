OGR / Python / PostGIS
======================
Some reflections, and stuff learned!


1. In order to get OGR to work with sosi-files, you will have to compile OGR manually
2. There is a -j flag to make that speeds up stuff!!
3. In order to get to the system-installed python-ogr-bindings in a virtualenv, you have to use the --system-site-packages to virtualenv
4. FYBA (used for SOSI in ogr) does not work with UTF-8 files!
5. To read a utf-8 file with BOM, use 
    with open(filename, 'r') as infile:
        unicode(infile.read(), 'utf-8-sig')
6. charsets are strange!
7. chardet for python is a good tool for detecting charsets
8. To use COPY with psycopg2, look at https://gist.github.com/jsheedy/ed81cdf18190183b3b7d
9. ogr wkb.dumps does not write ewkb (i.e. no SRID info), workaround:

    def dumps(shape, srid=4326):
        geos.WKBWriter.defaults['include_srid'] = True
        geos.lgeos.GEOSSetSRID(shape._geom, 4326)
        return shape.wkb_hex

10. remember to select all layers in an ogr source!
11. iterators are neat!
12. json.dumps by defaults prints utf-data as escaped stuff. use json.dumps(data, ensure_ascii=False) to get utf-8 strings in json
13. a charset-"error" can be due to bad input-data!
14. wkb is faster than WKT!
15. the attribute .is_valid on a shapely geom will tell you if a geom is valid, but how to get the reason it's invalid? you have to capture stderr!
16. for gdal ctypes: https://github.com/django/django/tree/master/django/contrib/gis/gdal
17. for geos ctypes: https://github.com/Toblerity/Shapely/blob/master/shapely/geos.py
18. geos C API: http://www.gdal.org/ogr__api_8h.html
20. shapefiles are by definition in latin-1. However, this can be deviated from. Look for a .cpg file, this should contain the charset
21. GeoDjango has actually implemented a lot of ctypes-stuff for gdal/ogr and geos
22. Multithreading in python is a bitch
