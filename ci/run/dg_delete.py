from ci.config import get_instance
from ci.db.pgdbhelper import PGDbHelper
from ci.db.sqa.access import SqaAccess
from ci.db.sqa.mapper import Mapper
from ci.db.sqa.models import DataGranule, Variable, Provider, RasterTile
from sqlalchemy import create_engine


__author__ = 'ujjwal'


#config = get_instance()
config_file = "/home/ujjwal/DPR_SM/python/dpr_sm/ingest/lis_config.yml"
config = get_instance(config_file=config_file)

logger = config.logger

pgdb_helper = PGDbHelper(config.pgsql_conn_str(), echo=config.logsql)

engine = create_engine(config.sqa_connection_string())
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
    import argparse

    parser = argparse.ArgumentParser("Enter variable and provider name")
    parser.add_argument("--variable", help="name of variable to delete", type=str)
    parser.add_argument("--provider", help="name of provider", type=str)

    args = parser.parse_args()
    var_name = args.variable
    provider_name = args.provider

    if provider_name is None or (len(provider_name) == 0):
        raise Exception("please specify --provider" % provider_name)

    if var_name is None or (len(provider_name) == 0):
        raise Exception("please specify --variable" % var_name)

    with SqaAccess(engine=engine) as sqa_access:
        provider = sqa_access.findOne(Provider, {'name': provider_name})
        variable = sqa_access.findOne(Variable, {'name': var_name})

        if provider is None:
            raise Exception("Could not find provider for %s" % provider_name)

        if variable is None:
            raise Exception("Could not find variable for %s" % var_name)

        #logger.info("Deleting granule %d - %s " % (granule.id, granule.name))
        granules = sqa_access.session.query(DataGranule).filter(DataGranule.variable == variable,
                                        DataGranule.provider == provider).all()

        logger.info("To delete %d granules" % len(granules))
        for granule in granules:
            tiles = sqa_access.session.query(RasterTile).filter(RasterTile.datagranule == granule).all()
            for tile in tiles:
                sqa_access.session.delete(tile)

            sqa_access.session.delete(granule)
            sqa_access.session.commit()

            logger.info("Deleted granule %d - %s " % (granule.id, granule.name))
