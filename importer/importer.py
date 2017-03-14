# -*- coding: utf-8 -*-
#from dotenv import load_dotenv, find_dotenv
#import psycopg2
import fiona
import gdal

#print gdal

#load_dotenv(find_dotenv())


# def read_file(filename):
#    with open(filename, 'r') as infile:
#        return infile.read()


if __name__ == '__main__':
    #c = fiona.open('data/d.geojson', 'r')
    #print len(c)
    #print c.crs
    #print c.schema
    pass
    from osgeo import ogr

    multipoint = ogr.Geometry(ogr.wkbMultiPoint)

    point1 = ogr.Geometry(ogr.wkbPoint)
    point1.AddPoint(1251243.7361610543, 598078.7958668759)
    multipoint.AddGeometry(point1)

    point2 = ogr.Geometry(ogr.wkbPoint)
    point2.AddPoint(1240605.8570339603, 601778.9277371694)
    multipoint.AddGeometry(point2)

    point3 = ogr.Geometry(ogr.wkbPoint)
    point3.AddPoint(1250318.7031934808, 606404.0925750365)
    multipoint.AddGeometry(point3)

    print multipoint.ExportToWkt()
