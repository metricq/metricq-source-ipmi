from setuptools import setup

setup(
    name="metricq_source_ipmi",
    version="0.1",
    author="TU Dresden",
    python_requires=">=3.6",
    packages=["metricq_source_ipmi.source", "metricq_source_ipmi.plugin_hyc_tenant"],
    scripts=[],
    entry_points="""
      [console_scripts]
      metricq-source-ipmi=metricq_source_ipmi.source:run
      """,
    install_requires=["aiomonitor", "click", "click_log", "metricq", "python-hostlist"],
    use_scm_version=True,
)
