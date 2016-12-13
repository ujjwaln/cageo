from ci.models.spatial_reference import SRID_RAP
from ci.db.pgdbhelper import PGDbHelper
from ci.config import get_instance


__author__ = 'ujjwal'


config = get_instance()
logger = config.logger
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=config.logsql)


sql = """
    DROP TABLE IF EXISTS rastertile_reproj;
    CREATE TABLE rastertile_reproj
    (
      id serial NOT NULL,
      rast raster NOT NULL,
      datagranule_id integer NOT NULL,
      CONSTRAINT rastertile_reproj_pkey PRIMARY KEY (id),
      CONSTRAINT rastertile_reproj_datagranule_id_fkey FOREIGN KEY (datagranule_id)
          REFERENCES datagranule (id) MATCH SIMPLE
          ON UPDATE NO ACTION ON DELETE NO ACTION
    )
    WITH (
      OIDS=FALSE
    );
"""
pgdb_helper.submit(sql)

sql = """
    SELECT datagranule.id, datagranule.srid FROM datagranule
    JOIN variable on variable.id=datagranule.variable_id
    WHERE variable.name <> 'MREFL'
"""
dg_rows = pgdb_helper.query(sql)

for dg_row in dg_rows:
    dg_id = dg_row[0]
    dg_srid = dg_row[1]

    sql = """
        INSERT INTO rastertile_reproj (rast, datagranule_id) (
            SELECT st_tile(st_transform(st_union(rast), %d), 1, 100, 100, TRUE, -999), %d
        FROM rastertile WHERE datagranule_id=%d)
    """ % (SRID_RAP, dg_id, dg_id)
    #
    # sql = """
    #     INSERT INTO rastertile_reproj (rast, datagranule_id) (
    #         SELECT st_transform(rast, %d), %d
    #         FROM rastertile WHERE datagranule_id=%d
    #     )
    # """ % (SRID_RAP, dg_id, dg_id)
    #
    pgdb_helper.submit(sql)
    logger.info("Reprojected granule %d" % dg_id)

sql = """
create index rastertile_reproj_rast_gist_idx on rastertile_reproj using GIST(ST_CONVEXHULL(rast));
"""
pgdb_helper.submit(sql)

# sql = """
# create index rastertile_reproj_dgid_idx on rastertile_reproj(datagranule_id);
# """
# pgdb_helper.submit(sql)
