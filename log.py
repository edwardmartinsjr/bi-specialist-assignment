import logging

# Set logger
logger = logging.getLogger(__name__)

def set_logger(debug):
    fh = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if debug is True:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
