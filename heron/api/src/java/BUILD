licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_library(
    name = "api-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//heron/proto:proto_topology_java",
        "//heron/proto:proto-java",
        "//3rdparty/kryo:kryo-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)
