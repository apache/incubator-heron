package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex/pex_rules", "pex_library")

pex_library(
    name = 'common-py',
    srcs = glob(['**/*.py']),
    deps = [
        '//heron/proto:proto-py',
    ],
    reqs = [
        'colorlog==2.6.1',
    ]
)
