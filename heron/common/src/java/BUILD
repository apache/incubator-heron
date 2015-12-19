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
    name = 'base-lib-java',
    srcs = glob(["**/core/base/*.java"]),
)

java_library(
    name = 'core-lib-java',
    srcs = glob(["**/core/**/*.java"]),
    deps = [
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_library(
    name = 'heron-utils-lib-java',
    srcs = glob(["**/utils/**/*.java"]),
    deps = [
        ":core-lib-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_physical_plan_java",
        "//heron/proto:proto_tuple_java",
        "//3rdparty/snakeyaml:snakeyaml-java",
    ],
)
