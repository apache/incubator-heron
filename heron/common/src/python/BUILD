package(default_visibility = ["//visibility:public"])

pex_library(
  name = 'common',
  srcs = glob(['**/*.py']),
  deps = [
    '//heron/proto:proto-py',
  ],
  reqs = [
    'colorlog==2.6.1',
    'tornado==4.0.2',
  ]
)

pex_library(
  name = 'tracker-access',
  srcs = glob(['**/handler/access/*.py']),
  deps = [],
)
