package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron_deps", "heron_java_proto_files")

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
        ":statemgr-spi-java",
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
    name='uploader-spi-java',
    srcs = glob([
        "**/spi/common/**/*.java",
        "**/spi/utils/**/*.java",
        "**/spi/newuploader/**/*.java",
    ]),
)

java_library(
    name='metricsmgr-spi-java',
    srcs = glob(
        ["**/spi/metricsmgr/**/*.java"],
    ),
)

statemgr_deps_files = \
    heron_java_proto_files() + [
        "//3rdparty/guava:guava-java",
    ]

java_library(
    name='statemgr-spi-java',
    srcs = glob(
        ["**/spi/statemgr/**/*.java"],
    ),
    deps = statemgr_deps_files,
)
