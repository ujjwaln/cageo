import logging
import os
from datetime import datetime

__author__ = 'ujjwal'

#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#We only want to see certain parts of the message
formatter = logging.Formatter(fmt='%(asctime)s:\t%(levelname)s:\t%(funcName)s:\t\t%(message)s', datefmt='%H:%M:%S')

dt = datetime.now()
filename = "ci_%s.log" % dt.strftime('%Y-%m-%d-%H-%M-%S')
filename = os.path.join(__file__, "../../../logs", filename)
file_handler = logging.FileHandler(filename, mode='w')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def set_log_level(level):
    if level.lower() == "info":
        logger.setLevel(logging.INFO)

    if level.lower() == "warning":
        logger.setLevel(logging.WARNING)

    if level.lower() == "critical":
        logger.setLevel(logging.CRITICAL)

    if level.lower() == "error":
        logger.setLevel(logging.ERROR)

    if level.lower() == "debug":
        logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    logger.debug('debug message')
    logger.info('info message')
    logger.warn('warn message')
    logger.error('error message')
    logger.critical('critical message')
