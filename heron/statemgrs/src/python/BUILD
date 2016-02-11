package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library")

pex_library(
  name = 'zk-statemgr-py',
  srcs = glob(['**/*.py']),
  deps = [
    '//heron/proto:proto-py',
  ],
  eggs = [
    '//3rdparty/eggs:kazoo',
    '//3rdparty/eggs:pyyaml',
    '//3rdparty/eggs:zope'
  ]
)
