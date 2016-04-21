package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto",  "proto_library")

proto_library(
    name="extra_actions_proto",
    src = "extra_actions_base.proto",
    gen_cc = 0,
    gen_java = 1,
    gen_py = 0,
)

java_library(
    name = "proto_java",
    deps = [
        ":extra_actions_proto_java",
        "@protobuf-java//jar",
    ],
    srcs = [
        "Empty.java",
    ],
)