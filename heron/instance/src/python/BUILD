package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = 'instance-py',
    srcs = glob(['**/*.py']),
    deps = [
        '//heronpy/proto:proto-py',
        '//heronpy/api:heron-python-py',
        '//heronpy/streamlet:heron-python-streamlet-py',
        '//heronpy/connectors:heron-pythonconnectors-py',
        '//heron/common/src/python:common-py',
    ],
)

# build binary for single thread python heron instance
pex_binary(
    name = "heron-python-instance",
    srcs = ["instance/st_heron_instance.py"],
    deps = [":instance-py"],
    reqs = [
        'colorlog==2.6.1',
        'PyYAML==3.10'
    ]
)
