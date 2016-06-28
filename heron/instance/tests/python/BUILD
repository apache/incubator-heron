load("/tools/rules/pex_rules", "pex_test")

pex_test(
  name = "protocol_unittest",
  srcs = ["protocol_unittest.py"],
  deps = [
    "//heron/instance/src/python:heron-nw-cli-lib",
  ],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
  ],
  size = "small",
)
