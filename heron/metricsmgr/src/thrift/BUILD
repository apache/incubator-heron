package(default_visibility = ["//visibility:public"])

load(
    "//tools/rules:genthrift.bzl",
    "thrift_library",
)

thrift_library(
    name = "thrift_scribe",
    src = "scribe.thrift",
    gen_java = 1,
)

java_library(
    name = "thrift-java",
    srcs = ["Empty.java"],
    deps = [":thrift_scribe_java"]
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
