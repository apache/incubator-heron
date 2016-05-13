package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
  name = 'common',
  srcs = glob(['**/*.py']),
  deps = [
    '//heron/proto:proto-py',
  ],
  eggs = [
    '//3rdparty/eggs:colorlog',
  ],
)

pex_library(
    name = "heron-network-lib",
    srcs = glob(
        ["network/*.py"],
        exclude = ["network/main.py"],
    ),
    deps = [
      "//heron/proto:proto-py",
      "//heron/statemgrs/src/python:zk-statemgr-py",
    ],
    eggs = [
        "//3rdparty/eggs:gevent",
    ]
)
