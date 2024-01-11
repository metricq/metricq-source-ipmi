import logging
import logging.handlers
import click_log
from metricq.logging import get_logger

logger = get_logger()

click_log.basic_config(logger)
sh = logging.handlers.SysLogHandler()
logger.addHandler(sh)
logger.setLevel('INFO')
logger.handlers[0].formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')

NEEDED_SENSORS = 4
POWER_SUPPLY_STATUS_OK = ["'OK'", "'Presence detected'"]

def create_metric_value(sensors):
    logger.info(f"sensors: {sensors}")
    if {'PS1_Status', 'PS2_Status', 'PS3_Status', 'PS4_Status'}.issubset(sensors.keys()) and len(sensors) == NEEDED_SENSORS:
        if sensors['PS1_Status']['status'] in POWER_SUPPLY_STATUS_OK and sensors['PS2_Status']['status'] in POWER_SUPPLY_STATUS_OK and sensors['PS3_Status']['status'] in POWER_SUPPLY_STATUS_OK and sensors['PS4_Status']['status'] in POWER_SUPPLY_STATUS_OK:
            return float(1)
        else:
            return float(0)
    return float('nan')
