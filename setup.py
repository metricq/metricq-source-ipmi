from setuptools import setup

setup(name='metricq_source_ipmi',
      version='0.1',
      author='TU Dresden',
      python_requires=">=3.5",
      packages=['metricq_source_ipmi'],
      scripts=[],
      entry_points='''
      [console_scripts]
      metricq-source-ipmi=metricq_source_ipmi:run
      ''',
      install_requires=['aiomonitor', 'click', 'click_log', 'metricq', 'python-hostlist'])
