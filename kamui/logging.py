import logging
import sys


def get_logger(name):
    fmt = logging.Formatter('%(asctime)s|%(process)d|%(name)s:%(lineno)s|%(levelname)s - %(message)s')
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(fmt)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stdout)
    return logger
