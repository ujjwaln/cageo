from datetime import datetime
from ci.models.raster import Raster
from ci.db.sqa.models import DataGranule, RasterTile, Mask
from ci.db.sqa.mapper import Mapper
from ci.db.sqa.access import SqaAccess
from ci.db.pgdbhelper import PGDbHelper


__author__ = 'ujjwal'


class RasterWriter(object):

    def __init__(self, config, engine, raster):
        if not isinstance(raster, Raster):
            raise Exception("raster is not Raster type")

        self.raster = raster
        self.table_name = None
        self.cls = None
        self.engine = engine
        self.config = config
        mapper = Mapper(engine=self.engine)
        mapper.map_tables()

    def write_to_pg_raster(self, provider_name, variable_name, granule_name, srid, level, start_time, end_time=None,
                           block_size=(100, 100), overwrite=False, threshold=None, mask_name=None):

        #self.cls = RasterTile
        with SqaAccess(engine=self.engine) as orm_access:
            provider = orm_access.find('provider', {'name': provider_name})[0]
            variable = orm_access.find('variable', {'name': variable_name})[0]

            extent = self.raster.wkt_extent()
            if end_time is None:
                end_time = datetime.max

            granule = DataGranule(provider=provider, variable=variable, starttime=start_time, endtime=end_time,
                                  extent=extent, level=level, name=granule_name, srid=srid, table_name="rastertile",
                                  file_name=self.raster.dsname)
            if overwrite:
                check_granule_result = orm_access.find(DataGranule,
                        filterr={'provider_id': provider.id, 'variable_id': variable.id, 'level': level,
                                 'starttime': start_time, 'endtime': end_time, 'file_name': self.raster.dsname})

                if len(check_granule_result):
                    check_granule = check_granule_result[0]
                    self.config.logger.warn('found existing datagranule %d' % check_granule.id)
                    rastertile_results = orm_access.find('rastertile',
                                        filterr={'datagranule_id': check_granule.id})
                    for rastertile in rastertile_results:
                        self.config.logger.warn('removing existing rastertile %d' % rastertile.id)
                        #orm_access.delete('rastertile', rastertile.id)
                        orm_access.session.delete(rastertile)
                        orm_access.session.commit()

                    #orm_access.delete(DataGranule, id=check_granule.id)
                    orm_access.session.delete(check_granule)
                    orm_access.session.commit()

            orm_access.insertOne(granule)
            tile_count = 0
            mask_wkt = None
            if mask_name:
                mask = orm_access.findOne(Mask, filterr={"name": mask_name})
                mask_wkt = orm_access.AsEWKT(mask.geom, srid)

            for tile in self.raster.tile_generator(block_size):
                tile_wkb = tile["wkb"]
                min_val = tile["min"]
                max_val = tile["max"]
                ext = tile["extent"]

                insert = False
                if threshold:
                    if max_val > threshold:
                        insert = True
                else:
                    insert = True

                if min_val == max_val and min_val == self.raster.nodata_value:
                    insert = False

                if insert and mask_wkt:
                    if orm_access.Intersects(ext, mask_wkt):
                        insert = True
                    else:
                        insert = False

                if insert:
                    rastertile = RasterTile(datagranule=granule, rast=tile_wkb)
                    orm_access.insertOne(rastertile)
                    tile_count += 1

            if tile_count > 0:
                self.config.logger.info("inserted %d tiles" % tile_count)
                return granule.id
            else:
                orm_access.delete(granule)
                return -1

    def write_to_pg_vector(self, provider_name, variable_name, granule_name, table_name, srid, level, start_time,
                           end_time=None, block_size=(100, 100), overwrite=False, threshold=None, mask_name=None):

        with SqaAccess(engine=self.engine) as orm_access:
            provider = orm_access.find('provider', {'name': provider_name})[0]
            variable = orm_access.find('variable', {'name': variable_name})[0]

            extent = self.raster.wkt_extent()
            if end_time is None:
                end_time = datetime.max

            granule = DataGranule(provider=provider, variable=variable, starttime=start_time, endtime=end_time,
                                  extent=extent, level=level, name=granule_name, srid=srid, table_name=table_name,
                                  file_name=self.raster.dsname)
            if overwrite:
                check_granule_result = orm_access.find(DataGranule,
                        filterr={'provider_id': provider.id, 'variable_id': variable.id, 'level': level,
                                 'starttime': start_time, 'endtime': end_time, 'file_name': self.raster.dsname})

                if len(check_granule_result):
                    check_granule = check_granule_result[0]
                    self.config.logger.warn('found existing datagranule %d' % check_granule.id)

                    sql = "drop table if exists %s;" % check_granule.table_name
                    orm_access.session.execute(sql)

                    #orm_access.delete(DataGranule, id=check_granule.id)
                    orm_access.session.delete(check_granule)
                    orm_access.session.commit()

            orm_access.insertOne(granule)

            pgdb_helper = PGDbHelper(conn_str=self.config.pgsql_conn_str(), echo=self.config.logsql)
            sql = """
                create table {table_name}
                (
                    id serial not null,
                    datagranule_id integer not null,
                    geom geometry not null,
                    value double precision,
                    CONSTRAINT {table_name}_pkey PRIMARY KEY (id)
                )
                """.format(table_name=table_name)
            #orm_access.session.execute(sql)
            pgdb_helper.submit(sql)

            values = []
            for shapes in self.raster.vector_generator(block_size=block_size):
                for shape in shapes:
                    values.append((granule.id, shape[0].ExportToWkt(), shape[1]))
                    if len(values) > 1000:
                        sql = """
                            insert into {table_name} (datagranule_id, geom, value) values (%s, st_geomfromtext(%s, 4326), %s)
                            """.format(table_name=table_name)
                        #orm_access.session.execute(sql, values)
                        pgdb_helper.insertMany(sql, values)
                        values = []

            if len(values) > 0:
                sql = """
                        insert into {table_name} (datagranule_id, geom, value) values (%s, st_geomfromtext(%s, 4326), %s)
                    """.format(table_name=table_name)
                pgdb_helper.insertMany(sql, values)

        sql = """
            create index {table_name}_geom_idx on {table_name} using GIST(geom)
            """.format(table_name=table_name)
        pgdb_helper.submit(sql)
