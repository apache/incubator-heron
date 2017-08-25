package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library")

pex_library(
    name = 'instance-py',
    srcs = glob(['**/*.py']),
    deps = [
        '//heron/proto:proto-py',
        '//heron/common/src/python:common-py'
    ],
    reqs = [
        'colorlog==2.6.1',
        'psutil==4.3.1',
    ]
)
