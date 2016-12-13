import glob
import os

from sqlalchemy import create_engine

from ci.db.pgdbhelper import PGDbHelper
from ci.db.sqa.access import SqaAccess
from ci.db.sqa.mapper import Mapper
from ci.ingest.raster_writer import RasterWriter

__author__ = 'ujjwal'


class BaseIngestor:

    def __init__(self, config):
        self.config = config

    def get_ingest_files(self, files_dir, wildcard="*"):
        pattern = os.path.join(self.config.datadir, files_dir, wildcard)
        files = glob.glob(pattern)
        files = sorted(files)
        if len(files):
            return files
        else:
            raise Exception("No files for pattern %s" % pattern)

    def get_ingest_file_path(self, filenamepart):
        return os.path.join(self.config.datadir, filenamepart)

    def ingest(self, ras, provider_name, variable_name, granule_name, table_name, srid, level, block_size, dynamic,
           start_time, end_time, subset_bbox=None, overwrite=False, threshold=None):

        if start_time is None or end_time is None:
            raise Exception("Starttime or endtime not specified")

        if (self.config.start_date > end_time) or (self.config.end_date < start_time):
            self.config.logger.info("Skipping dataset %s and it is not within time interval" % ras.dsname)
            return

        self.config.logger.info("Ingesting dataset %s, variable %s, level %d, start_time %s, end_time %s " %
          (ras.dsname, variable_name, level, start_time.strftime("%m-%d-%Y %H:%M"),end_time.strftime("%m-%d-%Y %H:%M")))

        if subset_bbox:
            try:
                ras.subset(subset_bbox)
            except:
                self.config.logger.warn("Non intersection with bbox")
                return

        mask_name = self.config.mask_name
        engine = create_engine(self.config.sqa_connection_string())
        writer = RasterWriter(config=self.config, engine=engine, raster=ras)

        granule_id = writer.write_to_pg_raster(provider_name=provider_name, variable_name=variable_name,
                granule_name=granule_name, srid=srid, level=level, start_time=start_time, end_time=end_time,
                block_size=block_size, overwrite=overwrite, threshold=threshold, mask_name=mask_name)

        return granule_id

    def ingest_vector(self, ras, provider_name, variable_name, granule_name, table_name, srid, level,
                      block_size, start_time, end_time, subset_bbox=None, overwrite=False, threshold=None):

        if start_time is None or end_time is None:
            raise Exception("Starttime or endtime not specified")

        if (self.config.start_date > end_time) or (self.config.end_date < start_time):
            self.config.logger.info("Skipping dataset %s and it is not within time interval" % ras.dsname)
            return

        self.config.logger.info("Ingesting dataset %s, variable %s, level %d, start_time %s, end_time %s " %
          (ras.dsname, variable_name, level, start_time.strftime("%m-%d-%Y %H:%M"),end_time.strftime("%m-%d-%Y %H:%M")))

        if subset_bbox:
            try:
                ras.subset(subset_bbox)
            except:
                self.config.logger.warn("Non intersection with bbox")
                return

        mask_name = self.config.mask_name
        engine = create_engine(self.config.sqa_connection_string())
        writer = RasterWriter(config=self.config, engine=engine, raster=ras)

        granule_id = writer.write_to_pg_vector(provider_name=provider_name, variable_name=variable_name,
                granule_name=granule_name, table_name=table_name, srid=srid, level=level, start_time=start_time,
                end_time=end_time, block_size=block_size, overwrite=overwrite, threshold=threshold,
                                               mask_name=mask_name)

        return granule_id

    def create_gist_index(self, table_name, index_name, column_name="rast"):
        pgdb_helper = PGDbHelper(conn_str=self.config.pgsql_conn_str())
        pgdb_helper.create_gist_index(table_name, index_name, column_name)

    def get_variables(self):
        engine = create_engine(self.config.sqa_connection_string())
        mapper = Mapper(engine=engine)
        mapper.map_tables()
        with SqaAccess(engine=engine) as sqa_access:
            vars = sqa_access.find('variable')

        return vars
