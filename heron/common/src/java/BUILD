licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name='common-java',
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/snakeyaml:snakeyaml-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_library(
    name = 'base-java',
    srcs = glob(["**/core/base/*.java"]),
)

java_library(
    name = 'basics-java',
    srcs = glob(["**/basics/*.java"]),
)

java_library(
    name = 'config-java',
    srcs = glob(["**/config/*.java"]),
    deps = [
        "//heron/common/src/java:basics-java",
        "//3rdparty/snakeyaml:snakeyaml-java",
    ],
)
