load("//tools/rules:genproto.bzl", "proto_library")
load("@rules_java//java:defs.bzl", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "extra_actions_proto",
    src = "extra_actions_base.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
)

java_library(
    name = "proto_java",
    srcs = ["Empty.java"],
    exports = [
        "@com_google_protobuf//:protobuf_java",
    ],
    deps = [
        ":extra_actions_proto_java",
        "@com_google_protobuf//:protobuf_java",
    ],
)
