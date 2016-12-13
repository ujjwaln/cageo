from osgeo import ogr, osr
from ci.util.logger import logger
from ci.util.proj_helper import get_bbox

__author__ = 'ujjwal'


class PointFeatureLayer(object):
    features = []

    def __init__(self, pgdb_helper, table_name, srid, recreate=True):
        self.pgdb_helper = pgdb_helper
        self.table_name = table_name
        if recreate:
            pgdb_helper.submit(
                """
                    drop table if exists %s;
                """ % table_name
            )
            pgdb_helper.submit(
                """
                    create table %s
                    (
                        id serial not null,
                        geom geometry not null,
                        value integer null
                    );
                """ % table_name
            )

        spRef = osr.SpatialReference()
        spRef.ImportFromEPSG(srid)
        self.spatial_reference = spRef
        self.bbox = get_bbox(srid=4326)

    def write_to_db(self, lats, lons, vals):
        values = []
        insert_sql = "insert into " + self.table_name + " (geom, value) values (ST_GeomFromText(%s, 4326), %s)"
        i = 0
        for lat in lats:
            lon = lons[i]

            if (lat > self.bbox[3]) and (lat < self.bbox[2]) and (lon > self.bbox[0]) and (lon < self.bbox[1]):
                f = ogr.Geometry(ogr.wkbPoint)
                f.AddPoint(lon, lat)
                values.append((f.ExportToWkt(), vals[i]))

            i += 1

        if len(values):
            self.pgdb_helper.insertMany(insert_sql, values)

        return len(values)

if __name__ == '__main__':
    import numpy

    lats = numpy.arange(34.5, 36.5, 0.1)
    lons = numpy.arange(-86, -84, 0.1)
    vals = numpy.arange(0, len(lats), 1)

    from ci.config import Config, get_env
    from ci.db.pg.PgDbHelper import PGDbHelper

    config = Config(env=get_env())
    pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)

    pfl = PointFeatureLayer(pgdb_helper=pgdb_helper, table_name='pointflayer', srid=4326, recreate=True)
    pfl.write_to_db(lats, lons, vals)
