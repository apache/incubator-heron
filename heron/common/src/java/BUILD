
licenses(["notice"])
package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
        "@org_yaml_snakeyaml//jar",
        "@com_google_guava_guava//jar",
    ]

java_library(
    name='common-java',
    srcs = glob(["**/*.java"]),
    deps = common_deps_files,
)

java_library(
    name = 'basics-java',
    srcs = glob(["**/basics/*.java"]),
    deps = [
        "@com_google_guava_guava//jar",
    ]
)

java_library(
    name = 'config-java',
    srcs = glob(["**/config/*.java"]),
    deps = [
        ":basics-java",
        "@org_yaml_snakeyaml//jar",
    ],
)

java_library(
    name = 'network-java',
    srcs = glob(["**/network/*.java"]),
    deps = [
        ":basics-java",
        "@com_google_protobuf_protobuf_java//jar",
    ],
)
