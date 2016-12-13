from sqlalchemy import create_engine
from ci.db.sqa.access import SqaAccess
from ci.db.pgdbhelper import PGDbHelper
from ci.db.sqa.mapper import Mapper
from ci.db.sqa.models import DataGranule, Variable, Provider
from ci.ingest import config, logger

__author__ = 'ujjwal'

engine = create_engine(config.sqa_connection_string())
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str())

mapper = Mapper(engine=engine)
mapper.map_tables()


def get_granules(var_name, start_date, end_date):
    sql = """
        select datagranule.id, datagranule.starttime, datagranule.endtime from datagranule
        join provider on provider.id = datagranule.provider_id
        join variable on variable.id = datagranule.variable_id
        where provider.name like 'RAP' and variable.name like '%s'
        and (('%s', '%s') overlaps (datagranule.starttime, datagranule.endtime))
        order by datagranule.starttime asc
    """ % (var_name, start_date, end_date)

    rows = pgdb_helper.query(sql)
    return rows


if __name__ == '__main__':
    logger.info("Ingesting RAP Derived granules")
    with SqaAccess(engine=engine) as sqa_access:

        rap_provider = sqa_access.findOne(Provider, {'name': 'RAP'})
        ugrd_var = sqa_access.findOne(Variable, {'name': 'UGRD'})
        vgrd_var = sqa_access.findOne(Variable, {'name': 'VGRD'})
        windconv_var = sqa_access.findOne(Variable, {'name': 'WINDCONV'})

        ugrd_granules = sqa_access.session.query(DataGranule).filter(DataGranule.variable == ugrd_var)\
            .order_by(DataGranule.starttime).all()
        logger.info("%d ugrd granules" % len(ugrd_granules))

        vgrd_granules = sqa_access.session.query(DataGranule).filter(DataGranule.variable == vgrd_var)\
            .order_by(DataGranule.starttime).all()

        logger.info("%d vgrd granules" % len(vgrd_granules))
        for i in range(0, len(ugrd_granules)):
            u_granule = ugrd_granules[i]
            v_granule = vgrd_granules[i]
            logger.info("Ingesting wind convergence for %s " % u_granule.name)

            if u_granule.starttime <> v_granule.starttime:
                raise Exception("u and v time mismatch %s, %s" % (u_granule.startime, v_granule.startime))

            #create temporary table for calculations
            sql = """
                drop table if exists tmpraster;
                create table tmpraster (
                    id int,
                    rast raster
                );
            """
            pgdb_helper.submit(sql)

            #insert du / dx
            sql = """
                INSERT INTO tmpraster (id, rast) VALUES (
                %d, (SELECT ST_MapAlgebraFctNgb(ST_Union(rast), 1, '32BF', 1, 1,
                'st_xderivative4ma(float[][],text,text[])'::regprocedure, 'ignore', (@ ST_ScaleX(ST_Union(rast)))::text)
                FROM rastertile  WHERE datagranule_id = %d))
            """ % (u_granule.id, u_granule.id)
            pgdb_helper.submit(sql)

            #insert dv / dy
            sql = """
                INSERT INTO tmpraster (id, rast) VALUES (
                %d, (SELECT ST_MapAlgebraFctNgb(ST_Union(rast), 1, '32BF', 1, 1,
                'st_yderivative4ma(float[][],text,text[])'::regprocedure, 'ignore', (@ ST_ScaleY(ST_Union(rast)))::text)
                FROM rastertile  WHERE datagranule_id = %d))
            """ % (v_granule.id, v_granule.id)
            pgdb_helper.submit(sql)

            granule_name = "%s_%s %s_%d" % (rap_provider.name, windconv_var.name,
                                            u_granule.starttime.strftime("%Y%m%d %H:%M"), 0)
            table_name = "%s_%s_%s_%d" % (rap_provider.name, windconv_var.name,
                                          u_granule.starttime.strftime("%Y%m%d %H:%M"), 0)

            granule = sqa_access.upsert_datagranule(rap_provider, windconv_var, granule_name, u_granule.starttime,
                                    u_granule.endtime, u_granule.extent, 0, u_granule.srid, table_name)

            sql = """
                insert into rastertile(rast, datagranule_id) values (
                 (select st_mapalgebra((select rast from tmpraster where id=%d), 1,
                                    (select rast from tmpraster where id=%d), 1,
                                    '[rast1.val]+[rast2.val]')
                 ), %d)
            """ % (u_granule.id, v_granule.id, granule.id)
            pgdb_helper.submit(sql)
