import importlib
import time
from queue import Queue
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

NaN = float('nan')
RETRY_INTERVALS = [5, 20, 60, 300]
LOADED_PLUGINS = {}


class ConfigError(Exception):
    pass


async def ipmi_sensors(hosts_list, username, password, record_ids=None):
    """call ipmi-sensors and parse the output

    :param hosts_list: List of hosts to be queried
    :param username: user name to query data
    :param password: password to query data
    :param record_ids: ids of the records that are queried
    :return: output table of ipmi-sensors
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
    query_timestamp = metricq.Timestamp.now()
    process = await asyncio.create_subprocess_exec(*param, stdout=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    output = stdout.decode()
    return query_timestamp, output


async def get_sensor_data_dict(hosts, username, password, record_ids=None):
    ts, data = await ipmi_sensors(
        hosts,
        username,
        password,
        record_ids=record_ids
    )

    data_dict = {}
    for elem in data.splitlines():
        row = elem.split('|')
        row = [cell.strip() for cell in row]
        record_id_host = row[0].split(': ')

        if len(record_id_host) > 1:
            host = record_id_host[0]
        else:
            host = hosts[0]
        if not row[1] in data_dict:
            data_dict[row[1]] = {}

        data_dict[row[1]][host] = {
            'record_id': record_id_host[-1],
            'category': row[2],
            'value': NaN if row[3] == 'N/A' else float(row[3]),
            'unit': row[4],
            'status': row[5],
        }
    return ts, data_dict


async def try_fix_hosts(conf):
    _, data = await get_sensor_data_dict(
        conf['hosts_with_error'].keys(),
        conf['username'],
        conf['password'],
    )
    to_del = []
    for host, host_name in conf['hosts_with_error'].items():
        for sensor in conf['sensors']:
            try:
                conf['record_ids'].add(
                    data[sensor][host]['record_id'],
                )
            except KeyError:
                break
        else:
            conf['hosts_names_dict'][host] = host_name
            to_del.append(host)
    for host in to_del:
        del conf['hosts_with_error'][host]
    if conf['hosts_with_error']:
        conf['next_fix_try'] = time.time() + \
            RETRY_INTERVALS[min(conf['next_fix_try'][1], len(RETRY_INTERVALS))]
        conf['next_fix_try'][1] += 1
    else:
        conf['next_fix_try'][1] = 0


async def collect_periodically(conf, result_queue):
    deadline = time.time() + conf['interval']
    while True:
        ts, data = await get_sensor_data_dict(
            conf['hosts_names_dict'].keys(),
            conf['username'],
            conf['password'],
            conf['record_ids'],
        )
        to_del = []
        for host, host_name in conf['hosts_names_dict'].items():
            sensors = {}
            for sensor in conf['sensors']:
                try:
                    sensors[sensor] = data[sensor][host]
                except KeyError:
                    logger.warning(
                        '{0} not found at {1}'.format(
                            sensor,
                            host_name,
                        )
                    )
                    conf['hosts_with_error'][host] = host_name
                    to_del.append(host)
                    sensors = {}
                    break
            value = NaN
            if sensors:
                if 'plugin' in conf:
                    try:
                        value = conf['plugin'].create_metric_value(sensors)
                    except Exception as e:
                        logger.error(
                            'Error in plugin, Exception: {0}'.format(
                                e,
                            )
                        )
                else:
                    value = sensors[conf['sensors'][0]]['value']

            metric_name = '{}.{}'.format(
                host_name,
                conf['metric_sufix']
            )
            result_queue.put((metric_name, ts, value))

        for host in to_del:
            del conf['hosts_names_dict'][host]

        if conf['hosts_with_error'] and time.time() > conf['next_fix_try'][0]:
            await try_fix_hosts(
                conf,
            )

        while deadline <= time.time():
            logging.warning('missed deadline')
            deadline += conf['interval']
        sleep_var = deadline - time.time()
        await asyncio.sleep(sleep_var)
        deadline += conf['interval']


def get_hostlist(obj):
    if type(obj) is str:
        return hostlist.expand_hostlist(obj)
    else:
        return obj


async def create_conf_and_metrics(conf_part, default_interval):
    new_confs_list = []
    metrics = {}
    hosts = get_hostlist(conf_part['hosts'])
    host_names = get_hostlist(conf_part['names'])
    _, queried_sensor_data = await get_sensor_data_dict(
        hosts,
        conf_part['username'],
        conf_part['password'],
    )

    if len(hosts) == len(host_names):
        hosts_names_dict = dict(zip(hosts, host_names))
        for metric_sufix, metric_data in conf_part['sensors'].items():

            interval = metric_data.get('interval', default_interval)

            new_metric_conf = {
                'metric_sufix': metric_sufix,
                'record_ids': set(),
                'hosts_names_dict': hosts_names_dict,
                'interval': interval,
                'username': conf_part['username'],
                'password': conf_part['password'],
                'hosts_with_error': {},
                'next_fix_try': (time.time(), 0),
            }

            if type(metric_data['name']) is str:
                new_metric_conf['sensors'] = [metric_data['name']]
            else:
                new_metric_conf['sensors'] = metric_data['name']

            needed_sensors = 1
            if 'plugin' in metric_data:
                if not metric_data['plugin'] in LOADED_PLUGINS:
                    full_modul_name = 'metricq_source_ipmi.plugin_{0}'\
                        .format(metric_data['plugin'])
                    if importlib.util.find_spec(full_modul_name):
                        LOADED_PLUGINS[metric_data['plugin']] = importlib.import_module(
                            full_modul_name,
                        )
                    else:
                        ConfigError(
                            'Plugin in {0} not found: {1}'
                            .format(
                                metric_sufix,
                                metric_data['plugin'],
                            )
                        )
                new_metric_conf['plugin'] = LOADED_PLUGINS[metric_data['plugin']]
                try:
                    needed_sensors = new_metric_conf['plugin'].NEEDED_SENSORS
                except AttributeError:
                    needed_sensors = None

            if needed_sensors:
                if len(new_metric_conf['sensors']) != needed_sensors:
                    raise ConfigError(
                        'Error: {} has not the right number of sensors(no plugin = 1)'
                        .format(metric_sufix)
                    )

            for host, host_name in hosts_names_dict.items():
                for sensor in new_metric_conf['sensors']:
                    try:
                        new_metric_conf['record_ids'].add(
                            queried_sensor_data[sensor][host]['record_id'],
                        )
                    except KeyError:
                        logger.warning(
                            'Sensor not found in {0}, {1}'
                            .format(
                                host,
                                sensor,
                            )
                        )
                        new_metric_conf['hosts_with_error'][host] = host_name
                        break

                metric_name = '{}.{}'.format(
                    host_name,
                    metric_sufix,
                )

                metrics[metric_name] = {
                    'rate': 1.0 / interval,
                    'unit': metric_data.get(
                        'unit',
                        'N/A',
                    )
                }
                if 'description' in metric_data:
                    metrics[metric_name]['description'] = metric_data['description']

            for host in new_metric_conf['hosts_with_error'].keys():
                del new_metric_conf['hosts_names_dict'][host]

            new_confs_list.append(new_metric_conf)
        return metrics, new_confs_list

    else:
        raise ConfigError(
            'number of names and hosts different in {} '.format(conf_part)
        )


class IpmiSource(metricq.IntervalSource):
    def __init__(self, *args, **kwargs):
        logger.info("initializing IpmiSource")
        super().__init__(*args, **kwargs)
        self.period = None
        self.result_queue = Queue()
        watcher = asyncio.FastChildWatcher()
        watcher.attach_loop(self.event_loop)
        asyncio.set_child_watcher(watcher)

    @metricq.rpc_handler('config')
    async def _on_config(self, **config):
        self.period = 1
        jobs = []
        for cfg in config['ipmi_hosts']:
            jobs.append(
                create_conf_and_metrics(
                    cfg,
                    config.get('interval', 1),
                )
            )
        if jobs:
            results = await asyncio.gather(*jobs)
        all_metrics = {}
        complete_conf = []
        for metrics, conf in results:
            all_metrics = {**all_metrics, **metrics}
            complete_conf.extend(conf)
        await self.declare_metrics(all_metrics)
        logger.info(
            "declared {} metrics".format(
                len(all_metrics),
            )
        )
        loops = []
        for conf in complete_conf:
            loops.append(
                collect_periodically(
                    conf,
                    self.result_queue,
                )
            )
        asyncio.gather(*loops)
        logger.info('{} loops started'.format(len(loops)))

    async def update(self):
        send_metrics = []
        while not self.result_queue.empty():
            metric_name, ts, value = self.result_queue.get()
            send_metrics.append(self[metric_name].send(ts, value))
        if send_metrics:
            ts_before = time.time()
            await asyncio.gather(*send_metrics)
            logger.info("Send took {:.2f} seconds, count: {}".format(
                time.time() - ts_before, len(send_metrics)))


@click.command()
@click.option('--server', default='amqp://localhost/')
@click.option('--token', default='source-ipmi')
@click_log.simple_verbosity_option(logger)
def run(server, token):
    src = IpmiSource(token=token, management_url=server)
    src.run()


if __name__ == '__main__':
    run()
