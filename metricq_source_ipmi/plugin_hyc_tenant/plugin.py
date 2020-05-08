NEEDED_SENSORS = 3
TENANT_STATUS_ENABLED = "'Device Enabled'"

def create_metric_value(sensors):
    if {'HYB_1 Tenant', 'HYB_2 Tenant'}.issubset(sensors.keys()) and len(sensors) == NEEDED_SENSORS:
        HYB_1 = float('nan')
        HYB_2 = float('nan')
        for sensor, sensor_data in sensors.items():
            if sensor in ['HYB_1 Tenant', 'HYB_2 Tenant']:
                continue
            if sensor.startswith('HYB_1'):
                HYB_1 = sensor_data['value']
            elif sensor.startswith('HYB_2'):
                HYB_2 = sensor_data['value']

        if sensors['HYB_1 Tenant']['status'] == TENANT_STATUS_ENABLED:
            return HYB_1
        elif sensors['HYB_2 Tenant']['status'] == TENANT_STATUS_ENABLED:
            return HYB_2
    return float('nan')
