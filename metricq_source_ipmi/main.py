import time
import logging
import logging.handlers
import asyncio
import click
import click_log

import metricq
from metricq.logging import get_logger
from test_conf import test_configs  # FIXME: for development only!

NaN = float('nan')

logger = get_logger()

click_log.basic_config(logger)
sh = logging.handlers.SysLogHandler()
logger.addHandler(sh)
logger.setLevel('INFO')
logger.handlers[0].formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')


async def get_ipmi_reading(cfg):
    query_timestamp = metricq.Timestamp.now()
    process = await asyncio.create_subprocess_exec(
        'ipmi-sensors',
        '-h', cfg['host'],
        '-u', cfg['username'],
        '-p', cfg['password'],
        '--no-header-output',
        '--session-timeout=950',
        '--retransmission-timeout=650',
        '-Q',
        stdout=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    output = stdout.decode().splitlines()
    output = [x.split('|') for x in output]
    ret = {}
    for elem in output:
        row = [y.strip() for y in elem]
        sensor = row[1]
        if sensor in cfg['sensor'].keys():
            metric_name = '{}.{}'.format(
                cfg['name'], cfg['sensor'][sensor]
            )
            value = float(row[3])
            ret[metric_name] = (query_timestamp, value)

    if len(cfg['sensor']) > len(ret):
        for sensor in cfg['sensor'].keys():
            metric_name = '{}.{}'.format(
                cfg['name'], cfg['sensor'][sensor]
            )
            if metric_name not in ret:
                ret[metric_name] = (query_timestamp, NaN)
    return ret


class IpmiSource(metricq.IntervalSource):
    def __init__(self, *args, **kwargs):
        logger.info("initializing IpmiSource")
        super().__init__(*args, **kwargs)
        self.period = None
        self.config = None

        watcher = asyncio.FastChildWatcher()
        watcher.attach_loop(self.event_loop)
        asyncio.set_child_watcher(watcher)

    @metricq.rpc_handler('config')
    async def _on_config(self, **config):
        rate = config.get('rate', 0.2)
        self.period = 1 / rate
        self.config = test_configs  # FIXME: change in Future
        metrics = {}
        for cfg in self.config:
            for sensor in self.config[cfg]['sensor']:
                metric_name = '{}.{}'.format(
                    cfg, self.config[cfg]['sensor'][sensor].replace('/', '.')
                )
                metrics[metric_name] = {'rate': rate}
        await self.declare_metrics(metrics)
        logger.info("declared {} metrics".format(len(metrics)))

    async def update(self):
        jobs = []
        for cfg in self.config:
            jobs.append(get_ipmi_reading(self.config[cfg]))
        data = await asyncio.gather(*jobs)
        send_metrics = []
        for data_row in data:
            for metric_name in data_row:
                ts, value = data_row[metric_name]
                print(metric_name, ts, value)
                send_metrics.append(self[metric_name].send(ts, value))
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
