import traceback
from sqlalchemy import create_engine
from ci.config import get_instance
from ci.db.adminpgdbhelper import AdminPGDbHelper
from ci.db.sqa.mapper import Mapper
from ci.db.sqa.access import SqaAccess

__author__ = 'ujjwal'


def create_tables(config):
    logger.info("Creating / Mapping tables on %s" % config.db["dbname"])

    engine = create_engine(config.sqa_connection_string())
    mapper = Mapper(engine=engine)

    #if tables don't exist map_tables will create them
    mapper.map_tables()

    logger.info("Inserting ref data on %s" % config.db["dbname"])

    with SqaAccess(engine=engine) as accessor:
        #importing ref data after tables have been mapped
        from ci.db.ref_data import VARIABLES, FORMATS, PROVIDERS

        logger.info("Inserting variables")
        accessor.insertMany(VARIABLES)

        logger.info("Inserting formats")
        accessor.insertMany(FORMATS)

        logger.info("Inserting providers")
        accessor.insertMany(PROVIDERS)


def create_new_geodb(config):
    logger.info("Creating new db %s" % config.db["dbname"])
    admin_db_helper = AdminPGDbHelper(conn_str=config.pgsql_postgres_conn_str())

    if admin_db_helper.check_database_exists(config.db["dbname"]):
        #logger.info("Deleting existing db %s" % config.db["dbname"])
        s = raw_input("Would you really like to delete the database %s ? y/n " % config.db["dbname"])
        if s != 'y':
            exit()
        try:
            admin_db_helper.drop_database(config.db["dbname"])
        except Exception, ex:
            logger.debug("Error while deleting existing db %s" % config.db["dbname"])
            logger.debug(traceback.print_exc())

    if not admin_db_helper.check_database_exists(config.db["dbname"]):
        try:
            admin_db_helper.create_database(config.db["dbname"])

            #connect to new db
            ci_db_helper = AdminPGDbHelper(config.pgsql_conn_str())

            logger.info("Enabling geodatabase on %s" % config.db["dbname"])
            ci_db_helper.enable_geodatabase()

            logger.info("Insert special coordinate systems %s" % config.db["dbname"])
            ci_db_helper.insert_sprefs()

            #insert custom functions for calculating du/dx, dv/dx
            logger.info("Inserting special plp/sql procedures %s" % config.db["dbname"])
            ci_db_helper.insert_plpsql_functions()

        except Exception, ex:
            logger.debug("Error while creating db %s" % config.db["dbname"])
            logger.debug(traceback.print_exc())


def insert_masks(config):
    from ci.db.ref_data import MASKINFOS
    from ci.util.shp_helper import ShapeFileHelper
    from ci.db.sqa.models import Mask

    engine = create_engine(config.sqa_connection_string())
    with SqaAccess(engine=engine) as sqa:
        for mask_info in MASKINFOS:

            if mask_info[0] == "shapefile":
                shp_helper = ShapeFileHelper(mask_info[2])
                for geom in shp_helper.wkt_geoms(4326):
                    ewkt_geom = geom
                    mask = Mask(name=mask_info[1], geom=ewkt_geom)
                    sqa.insertOne(mask)
                    break

            if mask_info[0] == "bbox":
                mask_name = mask_info[1]
                x_min = mask_info[2][0]
                y_max = mask_info[2][1]
                x_max = mask_info[2][2]
                y_min = mask_info[2][3]
                srid = mask_info[3]

                poly = "SRID=%d;POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))" % \
                       (srid, x_max, y_min, x_max, y_max, x_min, y_max, x_min, y_min, x_max, y_min)
                mask = Mask(name=mask_name, geom=poly)
                sqa.insertOne(mask)


if __name__ == '__main__':

    # config_file = "/home/ujjwal/DPR_SM/python/dpr_sm/ingest/lis_config.yml"
    # config = get_instance(config_file=config_file)

    config = get_instance()
    logger = config.logger

    create_new_geodb(config)
    create_tables(config)

    #connect to new db
    ci_db_helper = AdminPGDbHelper(config.pgsql_conn_str())
    logger.info("Create unique index on datagranule table")

    ci_db_helper.ensure_datagranule_unique_index()
    insert_masks(config)
