__author__ = 'ujjwal'

import json
from osgeo import osr, ogr
from ci.db.pgdbhelper import PGDbHelper
from ci.models.spatial_reference import SRID_WGS84


class ProjHelper:

    def __init__(self, config):
        self.config = config

    @staticmethod
    def latlon2xy1(lats, lons, srid_to_proj4):
        inSpRef = osr.SpatialReference()
        inSpRef.ImportFromEPSG(SRID_WGS84)

        outSpRef = osr.SpatialReference()
        outSpRef.ImportFromProj4(srid_to_proj4)

        coord_transform = osr.CoordinateTransformation(inSpRef, outSpRef)

        if isinstance(lats, list):
            i = 0
            x = []
            y = []
            for lat in lats:
                lon = lons[i]
                pt = ogr.Geometry(ogr.wkbPoint)
                pt.AddPoint(lon, lat)
                pt.Transform(coord_transform)

                x.append(pt.GetX())
                y.append(pt.GetY())

                i += 1

            return x, y
        else:
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.AddPoint(lons, lats)
            pt.Transform(coord_transform)
            return pt.GetX(), pt.GetY()

    def xy2latlon(self, x, y, srid_from):
        db = PGDbHelper(self.config.pgsql_conn_str())
        sql = """
            SELECT ST_AsGeoJSON(ST_Transform(ST_GeomFromText('POINT(%f %f)', %d), %d))
        """ % (x, y, srid_from, SRID_WGS84)
        results = db.query(sql)
        str_json = results[0][0]
        obj = json.loads(str_json)
        return tuple(obj["coordinates"])

    def latlon2xy(self, lat, lon, srid_to):
        db = PGDbHelper(self.config.pgsql_conn_str())
        sql = """
            SELECT ST_AsGeoJSON(ST_Transform(ST_GeomFromText('POINT(%f %f)', %d), %d))
        """ % (lon, lat, SRID_WGS84, srid_to)
        results = db.query(sql)
        str_json = results[0][0]
        obj = json.loads(str_json)
        return tuple(obj["coordinates"])

    def get_bbox(self, srid):

        lats = self.config.bbox["lats"]
        lons = self.config.bbox["lons"]

        _max_lat = lats[1] #35.1
        _min_lat = lats[0] #30
        _max_lon = lons[1] #-84.82
        _min_lon = lons[0] #-88.6

        _nw_xy = self.latlon2xy(_max_lat, _min_lon, srid)
        _sw_xy = self.latlon2xy(_min_lat, _min_lon, srid)
        _ne_xy = self.latlon2xy(_max_lat, _max_lon, srid)
        _se_xy = self.latlon2xy(_min_lat, _max_lon, srid)

        _bbox = min(_ne_xy[0], _nw_xy[0], _se_xy[0], _sw_xy[0]),\
                    max(_ne_xy[0], _nw_xy[0], _se_xy[0], _sw_xy[0]),\
                    max(_ne_xy[1], _nw_xy[1], _se_xy[1], _sw_xy[1]),\
                    min(_ne_xy[1], _nw_xy[1], _se_xy[1], _sw_xy[1])

        return _bbox


if __name__ == '__main__':
    from ci.models.spatial_reference import HRAP_Spatial_Reference
    ul_lat = 37
    ul_lon = -99.99
    ulx, uly = ProjHelper.latlon2xy1(ul_lat, ul_lon, srid_to_proj4=HRAP_Spatial_Reference.proj4)
    print ulx, uly
