
licenses(["notice"])
package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

java_library(
    name = 'basics-java',
    srcs = glob(["**/basics/*.java"]),
    deps = [
        "@com_google_guava_guava//jar",
        "//third_party/java:mockito",
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
        "//third_party/java:junit4",
        "@com_google_protobuf_protobuf_java//jar",
        "//heron/spi/src/java:utils-spi-java",
         "//third_party/java:jackson",
    ],
)

java_library(
    name = 'utils-java',
    srcs = glob(["**/utils/**/*.java"]),
    deps = heron_java_proto_files() + [
        ":basics-java",
        ":config-java",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:classification",
    ]
)