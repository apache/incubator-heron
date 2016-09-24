package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library")

pex_library(
    name = 'common-py',
    srcs = glob(['**/*.py']),
    deps = [
        '//heron/proto:proto-py',
    ],
    reqs = [
        'colorlog==2.6.1',
        'psutil==4.3.1',
    ]
)

pex_library(
    name = 'common-log-py',
    srcs = ["utils/log.py"],
    reqs = [
        "colorlog==2.6.1",
    ]
)
