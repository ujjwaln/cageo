__author__ = 'ujjwal'

from ci.config import get_instance
from ci.db.pgdbhelper import PGDbHelper
from ci.db.sqa.access import SqaAccess
from ci.db.sqa.mapper import Mapper
from ci.db.sqa.models import DataGranule, Variable, Provider, RasterTile
from sqlalchemy import create_engine


config = get_instance()

logger = config.logger
engine = create_engine(config.sqa_connection_string())
mapper = Mapper(engine=engine)
mapper.map_tables()

pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str())

with SqaAccess(engine=engine) as sqa:
    refl_reclass_vars = sqa.find(Variable, filterr={'name': 'REFL_reclass'})
    refl_var = sqa.find(Variable, filterr={'name': 'REFL'})[0]

    if len(refl_reclass_vars) == 0:
        refl_reclass_var = Variable(name='REFL_reclass', unit=None, description="Reclassified MRMS REFL")
        sqa.insertOne(refl_reclass_var)
    else:
        refl_reclass_var = refl_reclass_vars[0]

    mrms_provider = sqa.find(Provider, filterr={'name': 'MRMS'})[0]
    mrms_granules = sqa.find(DataGranule, filterr={'provider_id': mrms_provider.id, 'variable_id': refl_var.id})

    for g in mrms_granules:
        sql = """
            select id from datagranule where starttime=%s and endtime=%s and variable_id=%s and level=%s
            and provider_id=%s and file_name=%s
        """
        values = g.starttime, g.endtime, refl_reclass_var.id, g.level, mrms_provider.id, g.file_name
        rows = pgdb_helper.query(sql, values)

        if len(rows):
            sql = "delete from rastertile where datagranule_id=%d" % rows[0][0]
            pgdb_helper.submit(sql)

            sql = "delete from datagranule where id=%s" % rows[0][0]
            pgdb_helper.submit(sql)

        sql = """
                insert into datagranule (starttime, endtime, level, extent, name, srid, table_name, file_name,
                variable_id, provider_id) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        values = g.starttime, g.endtime, g.level, g.extent, g.name, g.srid, g.table_name, g.file_name, \
                 refl_reclass_var.id, mrms_provider.id
        dg_id = pgdb_helper.insertAndGetId(sql, values)

        sql = """
            insert into rastertile (datagranule_id, rast) values (%d, (
                select st_reclass(st_union(rast), 1, '[-100-35]:0, (35-100):1', '1BB', NULL)
                    from rastertile where datagranule_id=%d))
            """ % (dg_id, g.id)

        try:
            pgdb_helper.submit(sql)
        except:
            logger.critical("Error while executing %s" % sql)