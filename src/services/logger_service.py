import os
import sys
import logging
from logging import handlers


LOGDIR = os.path.join(os.getcwd(), 'logs')
LOGFILE = os.path.join(LOGDIR, 'output.log')

os.makedirs(LOGDIR, exist_ok=True)

LOG = logging.getLogger('DataPipeline')
LOG.setLevel(logging.INFO)
FORMAT = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

CH = logging.StreamHandler(sys.stdout)
CH.setFormatter(FORMAT)
LOG.addHandler(CH)

FH = handlers.RotatingFileHandler(LOGFILE, maxBytes=(1048576 * 5), backupCount=7)
FH.setFormatter(FORMAT)
LOG.addHandler(FH)


def info(message):
    LOG.info(message)

def error(message):
    LOG.error(message)

def exception(message):
    LOG.exception(message)