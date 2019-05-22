import logging
import logging.handlers
import asyncio
import click
from ClusterShell.NodeSet import NodeSet
import click_log

import metricq
from metricq.logging import get_logger

NaN = float('nan')

logger = get_logger()

click_log.basic_config(logger)
sh = logging.handlers.SysLogHandler()
logger.addHandler(sh)
logger.setLevel('INFO')
logger.handlers[0].formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')


async def ipmi_sensors(hosts_list, username, password, record_ids=None):
    """call ipmi-sensors and parse the output

    :param hosts_list: List of hosts to be queried
    :param username: user name to query data
    :param password: password to query data
    :param record_ids: ids of the records that are queried
    :return: list of the parsed output table of ipmi-sensors
    """

    nodeset = NodeSet().fromlist(hosts_list)
    param = [
        'ipmi-sensors',
        '-h', str(nodeset),
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


async def get_ipmi_reading(cfg):
    """

    :param cfg: config dict with:
                    hosts_names: dict:
                        key= host
                        value= name of the host
                    username: str username
                    password: str password
                    record_ids: set with the record_ids
                    sensor_names: dict:
                        key= sensor name
                        value= sensor metric name without host
    :return:
            dict:
                key= metric name
                value= timestamp, "value"
    """
    query_timestamp = metricq.Timestamp.now()
    parsed_output = await ipmi_sensors(cfg['hosts_names'].keys(), cfg['username'], cfg['password'], cfg['record_ids'])
    ret = {}
    for row in parsed_output:
        sensor = row[1]
        if sensor in cfg['sensor_names']:
            if len(row[0]) > 1:
                name = cfg['hosts_names'][row[0][0]]
            else:
                name = next(iter(cfg['hosts_names'].values()))
            metric_name = '{}.{}'.format(
                name, cfg['sensor_names'][sensor]
            )
            value = float(row[3])
            ret[metric_name] = (query_timestamp, value)

    if len(cfg['sensor_names'])*len(cfg['hosts_names']) > len(ret):
        for host_name in cfg['hosts_names'].values():
            for sensor in cfg['sensor_names'].values():
                metric_name = '{}.{}'.format(
                    host_name, sensor
                )
                if metric_name not in ret:
                    ret[metric_name] = (query_timestamp, NaN)
    return ret


async def get_record_ids(hosts, sensors, username, password):
    parsed_output = await ipmi_sensors(hosts, username, password)
    ret = set()
    for row in parsed_output:
        if row[1] in sensors:
            if len(row[0]) > 1:
                ret.add(row[0][1])
            else:
                ret.add(row[0][0])
    return ret


def get_list_from_conf(obj):
    if type(obj) is str:
        node_set = NodeSet(obj)
        return node_set
    else:
        return obj


class IpmiSource(metricq.IntervalSource):
    def __init__(self, *args, **kwargs):
        logger.info("initializing IpmiSource")
        super().__init__(*args, **kwargs)
        self.period = None
        self.config_optimized = None
        watcher = asyncio.FastChildWatcher()
        watcher.attach_loop(self.event_loop)
        asyncio.set_child_watcher(watcher)

    @metricq.rpc_handler('config')
    async def _on_config(self, **config):
        rate = config.get('rate', 0.2)
        self.period = 1 / rate
        self.config_optimized = []
        metrics = {}
        for cfg in config['ipmi_hosts']:
            updated_conf = {}
            hosts = get_list_from_conf(cfg['hosts'])
            names = get_list_from_conf(cfg['names'])

            if len(hosts) == len(names):
                updated_conf['username'] = cfg['username']
                updated_conf['password'] = cfg['password']
                updated_conf['hosts_names'] = dict(zip(hosts, names))
                updated_conf['sensor_names'] = {}
                for name in names:
                    for sensor in cfg['sensors']:
                        metric_name = '{}.{}'.format(
                            name,
                            sensor
                        )
                        updated_conf['sensor_names'][cfg['sensors'][sensor]['name']] = sensor
                        metrics[metric_name] = {'rate': rate}
                updated_conf['record_ids'] = await get_record_ids(
                    hosts,
                    updated_conf['sensor_names'].keys(),
                    cfg['username'],
                    cfg['password']
                )
                self.config_optimized.append(updated_conf)

            else:
                logger.error('ERROR number of names and hosts different in {} '.format(cfg))
                self.config_optimized = {}
        await self.declare_metrics(metrics)
        logger.info("declared {} metrics".format(len(metrics)))

    async def update(self):
        jobs = []
        for cfg in self.config_optimized:
            jobs.append(get_ipmi_reading(cfg))
        data = []
        if jobs:
            data = await asyncio.gather(*jobs)
        send_metrics = []
        if data:
            for data_row in data:
                for metric_name in data_row:
                    ts, value = data_row[metric_name]
                    send_metrics.append(self[metric_name].send(ts, value))
            if send_metrics:
                await asyncio.gather(*send_metrics)
            logger.info("sent {} metrics".format(len(send_metrics)))


@click.command()
@click.option('--server', default='amqp://localhost/')
@click.option('--token', default='source-ipmi')
@click_log.simple_verbosity_option(logger)
def source(server, token):
    src = IpmiSource(token=token, management_url=server)
    src.run()


if __name__ == '__main__':
    source()
