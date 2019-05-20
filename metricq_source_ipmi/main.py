import logging
import random
import asyncio
import click
import click_log

import metricq
from metricq.logging import get_logger
from test_conf import test_configs  # FIXME: for development only!

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel('INFO')
# Use this if we ever use threads
# logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s %(threadName)-16s %(levelname)-8s %(message)s')
logger.handlers[0].formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')


async def get_ipmi_reading(cfg):
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
    values = stdout.decode().splitlines()
    values = [x.split('|') for x in values]
    values_dict = {}
    for elem in values:
        row = [y.strip() for y in elem]
        sensor = row[1]
        if sensor in cfg['sensor'].keys():
            metric_name = '{}.{}'.format(
                cfg['name'], cfg['sensor'][sensor].replace('/', '.'))
            values_dict[metric_name] = row
    return values_dict


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
        rate = 2
        self.period = 1 / rate
        self.config = test_configs  # todo change in Future
        metrics = {}
        for cfg in self.config:
            for sensor in self.config[cfg]['sensor']:
                metric_name = '{}.{}'.format(
                    cfg, self.config[cfg]['sensor'][sensor].replace('/', '.'))
                metrics[metric_name] = {'rate': rate, 'description': ''}
        await self.declare_metrics(metrics)

    async def update(self):
        jobs = []
        for cfg in self.config:
            jobs.append(get_ipmi_reading(self.config[cfg]))
        data = await asyncio.gather(*jobs)
        print(data)
        # await self['test.py.dummy'].send(metricq.Timestamp.now(), random.random())


@click.command()
@click.option('--server', default='amqp://localhost/')
@click.option('--token', default='source-py-dummy')
@click_log.simple_verbosity_option(logger)
def source(server, token):
    src = IpmiSource(token=token, management_url=server)
    src.run()


if __name__ == '__main__':
    source()
