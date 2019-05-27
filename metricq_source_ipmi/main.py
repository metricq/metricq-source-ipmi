import math
import logging
import logging.handlers
import asyncio
import click
import click_log

import metricq
from metricq.logging import get_logger
import hostlist

IPMI_SENSORS = 'ipmi-sensors'
NaN = float('nan')

logger = get_logger()

click_log.basic_config(logger)
sh = logging.handlers.SysLogHandler()
logger.addHandler(sh)
logger.setLevel('INFO')
logger.handlers[0].formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')


def lcm(a, b):
    """Return lowest common multiple."""
    return a * b // math.gcd(a, b)


async def ipmi_sensors(hosts_list, username, password, record_ids=None):
    """call ipmi-sensors and parse the output

    :param hosts_list: List of hosts to be queried
    :param username: user name to query data
    :param password: password to query data
    :param record_ids: ids of the records that are queried
    :return: list of the parsed output table of ipmi-sensors
    """

    hosts = hostlist.collect_hostlist(hosts_list)
    param = [
        IPMI_SENSORS,
        '-h', hosts,
        '-u', username,
        '-p', password,
        '--no-header-output',
        '--session-timeout=950',
        '--retransmission-timeout=650',
        '-Q',
    ]

    if record_ids:
        param.extend(['-r', str.join(',', record_ids)])
    process = await asyncio.create_subprocess_exec(*param, stdout=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    output = stdout.decode().splitlines()
    for i, elem in enumerate(output):
        row = elem.split('|')
        row = [cell.strip() for cell in row]
        row[0] = row[0].split(': ')
        output[i] = row
    return output


async def get_ipmi_reading(cfg, current_iteration):
    """queries the host data and converts it to metrics

    :param cfg: config dict with:
                    hosts_names: dict:
                        key= host
                        value= name of the host
                    username: str username
                    password: str password
                    sensors: dict:
                        key= sensor name
                        value= dict:
                            metric_name= sensor metric name without host
                            interval= interval
                            record_ids= the record ids of the Sensor
    :param current_iteration: int of the current iteration
    :return:
            dict:
                key= metric name
                value= timestamp, "value"
    """
    query_timestamp = metricq.Timestamp.now()
    record_ids = set()
    sensor_names = {}
    for sensor in cfg['sensors']:
        if current_iteration % cfg['sensors'][sensor]['interval'] == 0:
            sensor_names[sensor] = cfg['sensors'][sensor]['metric_name']
            record_ids.update(cfg['sensors'][sensor]['record_ids'])
    parsed_output = await ipmi_sensors(cfg['hosts_names'].keys(), cfg['username'], cfg['password'], record_ids)
    ret = {}
    for row in parsed_output:
        sensor = row[1]
        if sensor in sensor_names:
            if len(row[0]) > 1:
                name = cfg['hosts_names'][row[0][0]]
            else:
                name = next(iter(cfg['hosts_names'].values()))
            metric_name = '{}.{}'.format(
                name, cfg['sensors'][sensor]['metric_name']
            )
            try:
                value = float(row[3])
            except ValueError:
                logger.error('ValueError by {} from {}'.format(sensor, name))
                value = NaN
            ret[metric_name] = (query_timestamp, value)
    if len(sensor_names)*len(cfg['hosts_names']) > len(ret):
        for host_name in cfg['hosts_names'].values():
            for sensor in cfg['sensors'].values():
                metric_name = '{}.{}'.format(
                    host_name, sensor['metric_name']
                )
                if metric_name not in ret:
                    ret[metric_name] = (query_timestamp, NaN)
    return ret


def search_sensor_unit(parsed_conf, name):
    for row in parsed_conf:
        if row[1] == name:
            return row[-2]


def get_list_from_conf(obj):
    if type(obj) is str:
        return hostlist.expand_hostlist(obj)
    else:
        return obj


def create_upd_conf_and_metrics(cfg, hosts, names, parsed_output, conf_interval):
    updated_conf = {}
    metrics = {}
    updated_conf['username'] = cfg['username']
    updated_conf['password'] = cfg['password']
    updated_conf['hosts_names'] = dict(zip(hosts, names))
    updated_conf['sensors'] = {}
    current_lcm = 1
    for name in names:
        for sensor in cfg['sensors']:
            metric_name = '{}.{}'.format(
                name,
                sensor
            )
            interval = int(cfg['sensors'][sensor].get('interval', conf_interval))
            updated_conf['sensors'][cfg['sensors'][sensor]['name']] = {
                'metric_name': sensor,
                'interval': interval,
                'record_ids': set()
            }

            current_lcm = lcm(current_lcm, interval)
            metrics[metric_name] = {
                'rate': interval,
                'unit': cfg['sensors'][sensor].get(
                    'unit',
                    search_sensor_unit(parsed_output, cfg['sensors'][sensor]['name'])
                )
            }
    for row in parsed_output:
        if row[1] in updated_conf['sensors'].keys():
            if len(row[0]) > 1:
                updated_conf['sensors'][row[1]]['record_ids'].add(row[0][1])
            else:
                updated_conf['sensors'][row[1]]['record_ids'].add(row[0][0])

    return metrics, updated_conf, current_lcm


class IpmiSource(metricq.IntervalSource):
    def __init__(self, *args, **kwargs):
        logger.info("initializing IpmiSource")
        super().__init__(*args, **kwargs)
        self.period = None
        self.config_optimized = []
        self.current_iteration = 0
        self.lcm = 1
        self.retry_intervals = [5, 20, 60, 300]
        watcher = asyncio.FastChildWatcher()
        watcher.attach_loop(self.event_loop)
        asyncio.set_child_watcher(watcher)

    async def declare_metrics_per_host(self, cfg, interval):
        metrics = {}
        number_of_trys = 0
        hosts = get_list_from_conf(cfg['hosts'])
        names = get_list_from_conf(cfg['names'])
        if len(hosts) == len(names):
            parsed_output = []
            while not parsed_output:
                parsed_output = await ipmi_sensors(hosts, cfg['username'], cfg['password'], )
                if parsed_output:
                    metrics, updated_conf, new_lcm = create_upd_conf_and_metrics(
                        cfg,
                        hosts,
                        names,
                        parsed_output,
                        interval
                    )
                    self.lcm = lcm(self.lcm, new_lcm)
                    self.config_optimized.append(updated_conf)
                else:
                    sleep_interval = min(number_of_trys, len(self.retry_intervals) - 1)
                    logger.error('no output of ipmi_sensors try again in {} sec]'.format(
                        self.retry_intervals[sleep_interval])
                    )
                    number_of_trys += 1
                    await asyncio.sleep(self.retry_intervals[sleep_interval])
        else:
            logger.error('number of names and hosts different in {} '.format(cfg))
            self.config_optimized = []
        await self.declare_metrics(metrics)
        logger.info("declared {} metrics".format(len(metrics)))

    @metricq.rpc_handler('config')
    async def _on_config(self, **config):
        self.period = 1
        self.config_optimized = []
        self.current_iteration = 0
        self.lcm = 1
        jobs = []
        for cfg in config['ipmi_hosts']:
            jobs.append(self.declare_metrics_per_host(cfg, config.get('interval', 1)))
        if jobs:
            await asyncio.gather(*jobs)

    async def update(self):
        jobs = []
        for cfg in self.config_optimized:
            if any(self.current_iteration % cfg['sensors'][sensor]['interval'] == 0 for sensor in cfg['sensors']):
                jobs.append(get_ipmi_reading(cfg, self.current_iteration))
        data = []
        if jobs:
            logger.info('start {} get_ipmi_reading jobs'.format(len(jobs)))
            data = await asyncio.gather(*jobs)
            logger.info('jobs finished')
        send_metrics = []
        if data:
            for data_row in data:
                for metric_name in data_row:
                    ts, value = data_row[metric_name]
                    send_metrics.append(self[metric_name].send(ts, value))
            if send_metrics:
                await asyncio.gather(*send_metrics)
            logger.info("sent {} metrics".format(len(send_metrics)))
        if self.current_iteration == self.lcm:
            self.current_iteration = 0
        self.current_iteration += 1


@click.command()
@click.option('--server', default='amqp://localhost/')
@click.option('--token', default='source-ipmi')
@click_log.simple_verbosity_option(logger)
def run(server, token):
    src = IpmiSource(token=token, management_url=server)
    src.run()


if __name__ == '__main__':
    run()
