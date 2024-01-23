NEEDED_SENSORS = 4
POWER_SUPPLY_STATUS_OK = ["'OK'", "'Presence detected'"]

def create_metric_value(sensors):
    if {'PS1_Status', 'PS2_Status', 'PS3_Status', 'PS4_Status'}.issubset(sensors.keys()) and len(sensors) == NEEDED_SENSORS:
        if sensors['PS1_Status']['status'] in POWER_SUPPLY_STATUS_OK and sensors['PS2_Status']['status'] in POWER_SUPPLY_STATUS_OK and sensors['PS3_Status']['status'] in POWER_SUPPLY_STATUS_OK and sensors['PS4_Status']['status'] in POWER_SUPPLY_STATUS_OK:
            return float(1)
        else:
            return float(0)
    return float('nan')
