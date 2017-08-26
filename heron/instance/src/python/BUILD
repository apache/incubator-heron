package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = 'instance-py',
    srcs = glob(['**/*.py']),
    deps = [
        '//heronpy/proto:proto-py',
        "//heronpy/api:heron-python-py",
        '//heron/common/src/python:common-py'
    ],
)

# build binary for single thread python heron instance
pex_binary(
    name = "heron-python-instance",
    srcs = ["instance/st_heron_instance.py"],
    deps = [":instance-py"],
    reqs = [
        'colorlog==2.6.1',
        'psutil==4.3.1',
        'PyYAML==3.10'
    ]
)
