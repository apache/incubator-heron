package(default_visibility = ["//visibility:public"])
load("/tools/rules/pex_rules", "pex_library", "pex_test")

pex_test(
  name = "serializer_unittest",
  srcs = ["serializer_unittest.py"],
  deps = [
    "//heron/instance/tests/python:pytest-lib",
  ],
  size = "small",
)

pex_test(
  name = "communicator_unittest",
  srcs = ["communicator_unittest.py"],
  deps = [
    "//heron/instance/tests/python:pytest-lib",
  ],
  size = "small",
)

pex_test(
  name = "pplan_helper_unittest",
  srcs = ["pplan_helper_unittest.py"],
  deps = [
    "//heron/instance/tests/python:pytest-lib",
  ],
  size = "small",
)

pex_test(
  name = "outgoing_tuple_helper_unittest",
  srcs = ["outgoing_tuple_helper_unittest.py"],
  deps = [
    "//heron/instance/tests/python:pytest-lib",
  ],
  size = "small",
)
