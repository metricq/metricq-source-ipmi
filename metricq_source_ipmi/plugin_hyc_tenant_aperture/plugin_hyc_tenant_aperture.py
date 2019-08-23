NEEDED_SENSORS = 4

def create_metric_value(sensors):
    if {'HYB_1 Pri Aper.', 'HYB_2 Pri Aper.', 'HYB_1 Tenant', 'HYB_2 Tenant'}.issubset(sensors.keys()):
        if sensors['HYB_1 Tenant']['status'] == "'Device Enabled'":
            return sensors['HYB_1 Pri Aper.']['value']
        elif sensors['HYB_2 Tenant']['status'] == "'Device Enabled'":
            return sensors['HYB_2 Pri Aper.']['value']
    return float('nan')
