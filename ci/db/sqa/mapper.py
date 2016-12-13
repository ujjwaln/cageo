from sqlalchemy.orm import relationship, mapper, clear_mappers
from sqlalchemy import Integer, Float, String, ForeignKey, Column, Table, DateTime, MetaData
from ci.db.sqa.types import PGGeometryType, PGRasterType, GeoJsonGeometryType
from ci.db.sqa.models import DataGranule, DataFormat, Variable, Provider, RasterTile, RoiGeom, RoiStat, Mask, RoiTrack
from geoalchemy2 import Geometry

__author__ = 'ujjwal'


class Mapper(object):

    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

    def map_tables(self):
        clear_mappers()

        dataformat = Table('dataformat', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('name', String(256), nullable=False))
        mapper(DataFormat, dataformat)

        variable = Table('variable', self.metadata,
                         Column('id', Integer, primary_key=True),
                         Column('name', String(256), nullable=False),
                         Column('unit', String(256), nullable=True),
                         Column('description', String(256), nullable=True))
        mapper(Variable, variable)

        provider = Table('provider', self.metadata,
                         Column('id', Integer, primary_key=True),
                         Column('name', String(256), nullable=False))
        mapper(Provider, provider)

        datagranule = Table('datagranule', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('starttime', DateTime, nullable=False),
                            Column('endtime', DateTime, nullable=False),
                            Column('level', Float, nullable=False),
                            Column('extent', PGGeometryType, nullable=False),
                            Column('name', String, nullable=False),
                            Column('srid', Integer, nullable=False),
                            Column('table_name', String, nullable=False),
                            Column('file_name', String, nullable=False),
                            Column('variable_id', Integer, ForeignKey('variable.id'), nullable=False),
                            Column('provider_id', Integer, ForeignKey('provider.id'), nullable=False))

        mapper(DataGranule, datagranule, properties={'variable': relationship(Variable), 'provider': relationship(Provider)})

        rastertile = Table('rastertile', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('rast', PGRasterType, nullable=False),
                           Column('datagranule_id', Integer, ForeignKey('datagranule.id'), nullable=False))

        mapper(RasterTile, rastertile, properties={'datagranule': relationship(DataGranule)})

        roi_geom = Table('roi_geoms', self.metadata,
                         Column('id', Integer, primary_key=True),
                         Column('roi_name', String),
                         Column('mrms_granule_id', Integer),
                         Column('starttime', DateTime),
                         Column('endtime', DateTime),
                         Column('geom', GeoJsonGeometryType),
                         Column('center',GeoJsonGeometryType),
                         Column('storm_poly', GeoJsonGeometryType),
                         Column('center_lat', Float),
                         Column('center_lon', Float),
                         Column('iarea', Float),
                         Column('type', Integer))
        mapper(RoiGeom, roi_geom)

        # roi_stat = Table('roi_stats', self.metadata,
        #                  Column('id', Integer, primary_key=True),
        #                  Column('roi_id', Integer),
        #                  Column('roi_name', String),
        #                  Column('granule_id', Integer),
        #                  Column('count', Integer),
        #                  Column('sum', Float),
        #                  Column('mean', Float),
        #                  Column('stddev', Float),
        #                  Column('min', Float),
        #                  Column('max', Float),
        #                  Column('variable_id', Integer))
        # mapper(RoiStat, roi_stat)

        roi_stat = Table('roi_stats', self.metadata,
                         Column('id', Integer, primary_key=True),
                         Column('roi_id', Integer),
                         Column('roi_name', String),
                         Column('count', Integer),
                         Column('sum', Float),
                         Column('mean', Float),
                         Column('stddev', Float),
                         Column('min', Float),
                         Column('max', Float),
                         Column('variable_id', Integer))
        mapper(RoiStat, roi_stat)

        mask = Table('mask', self.metadata,
                     Column('name', String, nullable=False, primary_key=True),
                     Column('geom', Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False))
        mapper(Mask, mask)

        # roi_track = Table('roi_track', self.metadata,
        #                   Column('id', Integer, primary_key=True),
        #                   Column('name', String),
        #                   Column('dtime', DateTime),
        #                   Column('center', Geometry(geometry_type="GEOMETRY", srid=4326))
        #                   )
        # mapper(RoiTrack, roi_track)

        self.metadata.create_all(self.engine)

    def create_rastertile_table(self, table_name, class_name):
        self.map_tables()
        rastertile = Table(table_name, self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('rast', PGRasterType, nullable=True),
                           Column('datagranule_id', Integer, ForeignKey('datagranule.id'), nullable=False))

        self.metadata.create_all(self.engine)
        #if class_name:

        cls = type(class_name, (RasterTile, ), {})
        mapper(cls, rastertile, properties={'datagranule': relationship(DataGranule)})

        return cls
