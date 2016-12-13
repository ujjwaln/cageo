__author__ = 'ujjwal'
import os
from osgeo import ogr, osr


class ShapeFileHelper(object):
    def __init__(self, filename):
        if not os.path.exists(filename):
            raise Exception("%f not found" % filename)

        fname, ext = os.path.splitext(filename)
        prjfile = "%s.prj" % fname

        driver = ogr.GetDriverByName("ESRI Shapefile")
        self.dataSource = driver.Open(filename, 0)
        self.layer = self.dataSource.GetLayer()

        prj_file = open(prjfile, 'r')
        prj_txt = prj_file.read()
        self.srs = osr.SpatialReference()
        self.srs.ImportFromESRI([prj_txt])
        self.srs.AutoIdentifyEPSG()

    def wkt_geoms(self, out_srid):
        transform = None
        srs_out = osr.SpatialReference()
        srs_out.ImportFromEPSG(out_srid)

        if not srs_out.IsSame(self.srs):
            transform = osr.CoordinateTransformation(self.srs, srs_out)

        for feature in self.layer:
            geom = feature.GetGeometryRef()
            if transform:
                geom.Transform(transform)

            wkt = geom.ExportToWkt()
            ewkt = "SRID=%d;%s" % (out_srid, wkt)
            yield ewkt
