import importlib
import time
from queue import Queue
from enum import Enum
import logging
import logging.handlers
import asyncio
import click
import click_log

import metricq
from metricq.logging import get_logger
import hostlist

IPMI_SENSORS = 'ipmi-sensors'
DEFAULT_PARAMS = {
    '--no-header-output': None,
    '--session-timeout': 950,
    '--retransmission-timeout': 650,
    '-Q': None,
}

CMD_IPMI_SENSORE_BASE = {}

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


class Status(Enum):
    ERROR = 0,
    ACTIVE = 1,


async def ipmi_sensors(hosts_list, username, password, record_ids=None):
    """call ipmi-sensors and parse the output

    :param hosts_list: List of hosts to be queried
    :param username: user name to query data
    :param password: password to query data
    :param record_ids: ids of the records that are queried
    :return: output table of ipmi-sensors
    """

    hosts = hostlist.collect_hostlist(hosts_list)
    fanout = str(min(1024, len(hosts_list)))
    param = [
        '-h', hosts,
        '-u', username,
        '-p', password,
        '-F', fanout,
    ]
    if record_ids:
        param.extend(['-r', str.join(',', record_ids)])
    query_timestamp = metricq.Timestamp.now()
    process = await asyncio.create_subprocess_exec(
        *CMD_IPMI_SENSORE_BASE,
        *param,
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    output = stdout.decode()
    return query_timestamp, output


async def get_sensor_data_dict(hosts, username, password, record_ids=None):
    ts, data = await ipmi_sensors(
        hosts,
        username,
        password,
        record_ids=record_ids,
    )

    data_dict = {}
    for elem in data.splitlines():
        row = elem.split('|')
        row = [cell.strip() for cell in row]
        if len(row) < 6:
            return ts, {}
        record_id_host = row[0].split(': ')

        if len(record_id_host) > 1:
            host = record_id_host[0]
        else:
            host = list(hosts)[0]
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


async def try_fix_hosts(conf, hosts_to_fix):
    now = time.time()
    _, data = await get_sensor_data_dict(
        hosts_to_fix,
        conf['username'],
        conf['password'],
    )
    for host in hosts_to_fix:
        for metric_sufix, metric_data in conf['metrics'].items():
            for sensor in metric_data['sensors']:
                try:
                    conf['record_ids'].add(
                        data[sensor][host]['record_id'],
                    )
                except KeyError:
                    conf['hosts'][host]['next_try'] = now + \
                        RETRY_INTERVALS[
                            min(conf['hosts'][host]['number_of_trys'],
                                len(RETRY_INTERVALS)-1)
                        ]
                    conf['hosts'][host]['number_of_trys'] += 1
                    break
            else:
                continue
            break
        else:
            conf['hosts'][host]['status'] = Status.ACTIVE
            conf['hosts'][host]['number_of_trys'] = 0
            conf['active_hosts'].add(host)


async def log_loop(configs, log_interval):
    while True:
        for conf in configs:
            active_queried_hosts = sum(
                1 for i in conf['hosts'].values() if i['status'] == Status.ACTIVE
            )
            logger.info(
                '{0} of {1} are active in {2}'.format(
                    active_queried_hosts,
                    len(conf['hosts']),
                    hostlist.collect_hostlist(conf['hosts'].keys()),
                )
            )
        await asyncio.sleep(log_interval)


async def collect_periodically(conf, result_queue):
    deadline = time.time() + conf['interval']
    while True:
        ts = metricq.Timestamp.now()
        if conf['active_hosts']:
            ts, data = await get_sensor_data_dict(
                conf['active_hosts'],
                conf['username'],
                conf['password'],
                conf['record_ids'],
            )
        hosts_to_fix = set()

        for host, host_info in conf['hosts'].items():
            for metric_sufix, metric_data in conf['metrics'].items():
                value = NaN
                if host_info['status'] == Status.ACTIVE:
                    sensors = {}
                    for sensor in metric_data['sensors']:
                        try:
                            sensors[sensor] = data[sensor][host]
                        except KeyError:
                            if host in conf['active_hosts']:
                                conf['active_hosts'].remove(host)
                            conf['hosts'][host]['status'] = Status.ERROR
                            hosts_to_fix.add(host)
                            sensors = {}
                            break
                    if not sensors:
                        continue
                    if 'plugin' in metric_data:
                        try:
                            value = metric_data['plugin'].create_metric_value(sensors)
                        except Exception as e:
                            logger.error(
                                'Error in plugin, Exception: {0}'.format(
                                    e,
                                )
                            )
                    else:
                        value = sensors[metric_data['sensors'][0]]['value']
                    value = value + metric_data['offset']

                elif host_info['status'] == Status.ERROR:
                    if not host in hosts_to_fix and time.time() > host_info['next_try']:
                        hosts_to_fix.add(host)

                metric_name = '{}.{}'.format(
                    host_info['host_name'],
                    metric_sufix,
                )
                result_queue.put((metric_name, ts, value))

        if hosts_to_fix:
            await try_fix_hosts(
                conf,
                hosts_to_fix,
            )

        while deadline <= time.time():
            logging.warning(
                'missed deadline in {}'.format(
                    hostlist.collect_hostlist(conf['hosts'].keys()),
                ),
            )
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
    metrics = {}
    hosts = get_hostlist(conf_part['hosts'])
    host_names = get_hostlist(conf_part['names'])
    _, queried_sensor_data = await get_sensor_data_dict(
        hosts,
        conf_part['username'],
        conf_part['password'],
    )

    if len(hosts) == len(host_names):
        interval = conf_part.get('interval', default_interval)
        new_conf = {
            'metrics': {},
            'record_ids': set(),
            'hosts': {},
            # 'active_hosts' serves for performance.
            # That not always an additional loop has to be made to check who is active.
            'active_hosts': set(),
            'interval': interval,
            'username': conf_part['username'],
            'password': conf_part['password'],
        }


        for host, host_name in zip(hosts, host_names):
            new_conf['hosts'][host] = {
                'host_name': host_name,
                'next_try': time.time(),
                'number_of_trys': 0,

            }
        for metric_sufix, metric_data in conf_part['sensors'].items():
            new_conf['metrics'][metric_sufix] = {}

            if type(metric_data['name']) is str:
                new_conf['metrics'][metric_sufix]['sensors'] = [
                    metric_data['name']]
            else:
                new_conf['metrics'][metric_sufix]['sensors'] = metric_data['name']

            new_conf['metrics'][metric_sufix]['offset'] = metric_data.get('offset', 0)

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
                new_conf['metrics'][metric_sufix]['plugin'] = LOADED_PLUGINS[metric_data['plugin']]
                try:
                    needed_sensors = new_conf['metrics'][metric_sufix]['plugin'].NEEDED_SENSORS
                except AttributeError:
                    needed_sensors = None

            if needed_sensors:
                if len(new_conf['metrics'][metric_sufix]['sensors']) != needed_sensors:
                    raise ConfigError(
                        'Error: {} has not the right number of sensors(no plugin = 1)'
                        .format(metric_sufix)
                    )

            for host, host_name in zip(hosts, host_names):
                for sensor in new_conf['metrics'][metric_sufix]['sensors']:
                    try:
                        new_conf['record_ids'].add(
                            queried_sensor_data[sensor][host]['record_id'],
                        )
                    except KeyError:
                        new_conf['hosts'][host]['status'] = Status.ERROR
                        if host in new_conf['active_hosts']:
                            new_conf['active_hosts'].remove(host)
                        break
                else:
                    new_conf['hosts'][host]['status'] = Status.ACTIVE
                    new_conf['active_hosts'].add(host)

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

        return metrics, new_conf

    else:
        raise ConfigError(
            'number of names and hosts different in {} '.format(conf_part)
        )


def build_cmd_ipmi_base(ipmi_sensors_cmd, changed_param):
    command = [ipmi_sensors_cmd]
    param = {}
    param.update(DEFAULT_PARAMS)
    param.update(changed_param)
    for tag, value in param.items():
        if tag.startswith('--') and value is None:
            command.append(
                tag,
            )
        elif tag.startswith('--'):
            command.append(
                '{tag}={value}'.format(
                    tag=tag,
                    value=value,
                ),
            )
        elif tag.startswith('-') and value is None:
            command.append(
                tag,
            )
        elif tag.startswith('-'):
            command.append(
                tag,
            )
            command.append(
                value,
            )
    return command


class IpmiSource(metricq.IntervalSource):
    def __init__(self, *args, **kwargs):
        logger.info("initializing IpmiSource")
        super().__init__(*args, **kwargs)
        self.result_queue = Queue()
        watcher = asyncio.FastChildWatcher()
        watcher.attach_loop(self.event_loop)
        asyncio.set_child_watcher(watcher)

    @metricq.rpc_handler('config')
    async def _on_config(self, **config):
        self.period = 1
        jobs = []
        global CMD_IPMI_SENSORE_BASE
        CMD_IPMI_SENSORE_BASE = build_cmd_ipmi_base(
            config.get("ipmi_sensors_cmd", IPMI_SENSORS),
            config.get("ipmi_sensors_params", {}),
        )

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
            complete_conf.append(conf)
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
        loops.append(
            log_loop(complete_conf, config.get('log_interval', 30)),
        )
        asyncio.gather(*loops)
        logger.info('{} loops started'.format(len(loops)))

    async def update(self):
        send_metric_count = 0
        while not self.result_queue.empty():
            metric_name, ts, value = self.result_queue.get()
            self[metric_name].append(ts, value)
            send_metric_count += 1
        ts_before = time.time()
        try:
            await self.flush()
        except Exception as e:
            logger.error("Exception in send: {}".format(str(e)))
        logger.info(
            "Send took {:.2f} seconds, count: {}".format(
                time.time() - ts_before,
                send_metric_count,
            ),
        )


@click.command()
@click.option('--server', default='amqp://localhost/')
@click.option('--token', default='source-ipmi')
@click_log.simple_verbosity_option(logger)
def run(server, token):
    src = IpmiSource(token=token, management_url=server)
    src.run()


if __name__ == '__main__':
    run()
