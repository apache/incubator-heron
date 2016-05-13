package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library")

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
    ),
    deps = [
      "//heron/proto:proto-py",
    ],
    eggs = [
        "//3rdparty/eggs:gevent",
    ]
)
