package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library")

pex_library(
  name = 'common',
  srcs = glob(['**/*.py']),
  deps = [
    '//heron/proto:proto-py',
  ],
  reqs = [
    'colorlog==2.6.1',
    'tornado==4.0.2',
    'pyyaml==3.10',
  ]
)

pex_library(
  name = 'tracker-access',
  srcs = glob(['**/handler/access/*.py']),
  deps = [],
)

pex_library(
  name = 'common_log',
  srcs = ["utils/log.py"],
  reqs = [
    "colorlog==2.6.1",
  ]
)

# This is common package for pyheron
pex_library(
  name = 'pyheron_common',
  srcs = glob(['**/*.py'],
              exclude=['utils/config.py', 'utils/tracker_access.py', 'handler/**/*.py']),
  reqs = ['colorlog==2.6.1'],
)