import yaml
import socket
from ci.util.logger import logger

__author__ = 'ujjwal'


__instance = None


class _Config(object):

    def __init__(self, config_file):
        config_file = config_file
        stream = file(config_file, 'r')
        obj = yaml.load(stream)

        self.db = {
            "username": obj["username"],
            "password": obj["password"],
            "dbname": obj["dbname"],
            "servername": obj["servername"],
            "port": obj["port"],
            "adminusername": obj["adminusername"],
            "adminpassword": obj["adminpassword"],
        }

        self.logsql = obj["logsql"]
        self.parallel = obj["parallel"]
        self.nprocs = obj["nprocs"]

        self.datadir = obj["datadir"]
        self.start_date = obj["start_date"]
        self.end_date   = obj["end_date"]

        self.datafiles = obj["datafiles"]
        self.mask_name = obj.get("mask_name", None)
        _bbox_name = obj.get("bbox_name", None)
        self.bbox = obj["bbox"][_bbox_name]

        self.log_level = obj["log_level"]
        self.logger = logger

        self.ci_lifetime_hours = obj["ci_lifetime_hours"]
        self.ci_roi_radius = obj["ci_roi_radius"]
        self.ci_threshold_dbz = obj["ci_threshold_dbz"]

        #self.forecast_times = obj.get("forecast_times", [])

    def sqa_connection_string(self):
        return 'postgresql://%s:%s@%s:%d/%s' % \
            (self.db["username"], self.db["password"], self.db["servername"], self.db["port"], self.db["dbname"])

    def ogr_connection_string(self):
        return "PG:dbname='%s' user='%s' password='%s'" % \
               (self.db["dbname"], self.db["username"], self.db["password"])

    def pgsql_conn_str(self):
        return "host=%s dbname=%s user=%s password=%s" % \
               (self.db["servername"], self.db["dbname"], self.db["username"], self.db["password"])

    def pgsql_postgres_conn_str(self):
        return "host=%s dbname=%s user=%s password=%s" % \
               ("localhost", "postgres", self.db["adminusername"], self.db["adminpassword"])


def get_instance(config_file=None):
    global __instance
    if __instance is None:
        if config_file is None:
            import os
            config_file_env = os.environ.get("CONFIG_FILE")
            if config_file_env is None:
                config_file = os.path.join(os.path.dirname(__file__), 'dev_july.yml')
                #config_file = os.path.join(os.path.dirname(__file__), 'prod_aug.yml')
            else:
                config_file = os.path.join(os.path.dirname(__file__), config_file_env)

            if not os.path.exists(config_file):
                config_file = os.path.join(os.path.dirname(__file__), config_file)
                if not os.path.exists(config_file):
                    raise Exception("Cannot find config file %s" % config_file)
            #logger.info("Using config file %s" % config_file)
        __instance = _Config(config_file)
    return __instance


def get_env():
    hostname = socket.gethostname()
    if 'cedarkey' in hostname:
        return 'production'
    else:
        return 'development'
