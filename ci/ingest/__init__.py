__author__ = 'ujjwal'

from ci.config import get_instance
from ci.ingest.base_ingestor import BaseIngestor
from ci.util.proj_helper import ProjHelper

config = get_instance()
logger = config.logger
proj_helper = ProjHelper(config=config)
base_ingestor = BaseIngestor(config=config)
