from setuptools import setup

setup(
    name="metricq_source_ipmi",
    version="0.1",
    author="TU Dresden",
    python_requires=">=3.10",
    packages=["metricq_source_ipmi.source", "metricq_source_ipmi.plugin_hyc_tenant", "metricq_source_ipmi.plugin_power_supply"],
    scripts=[],
    entry_points="""
      [console_scripts]
      metricq-source-ipmi=metricq_source_ipmi.source:run
      """,
    install_requires=[
        "aiomonitor",
        "click",
        "click_log",
        "metricq ~= 4.0",
        "python-hostlist",
    ],
    use_scm_version=True,
)
