licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "localmode-lib-java",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_stmgr_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/snakeyaml:snakeyaml-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)
