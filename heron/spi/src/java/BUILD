package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_library(
    name = 'scheduler-spi-java',
    srcs = glob([
        "**/spi/common/**/*.java",
        "**/spi/packing/**/*.java",
        "**/spi/scheduler/**/*.java",
        "**/spi/uploader/**/*.java",
        "**/spi/util/**/*.java",
    ]),
    deps = [
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/proto:proto_topology_java",
        "//3rdparty/snakeyaml:snakeyaml-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_library(
    name='metricsmgr-spi-java',
    srcs = glob(
        ["**/spi/metricsmgr/**/*.java"],
    ),
)
