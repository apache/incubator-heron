package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_test")

pex_test(
  name = "blobstore_unittest",
  srcs = ["store/blobstore_unittest.py"],
  deps = ["//heron/packer/src/python:heron-packer-lib"],
  reqs = [
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
    "testfixtures==4.10.0",
  ],
  size = "small",
)

pex_test(
  name = "metastore_unittest",
  srcs = ["store/metastore_unittest.py"],
  deps = ["//heron/packer/src/python:heron-packer-lib"],
  reqs = [
    "mock==1.0.1",
    "py==1.4.27",
    "pytest==2.6.4",
    "unittest2==0.5.1",
    "testfixtures==4.10.0",
  ],
  size = "small",
)