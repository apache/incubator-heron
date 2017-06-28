
licenses(["notice"])
package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

# basics-java only depends on jdk.
# No other heron code and 3rdparty dependencies should be added
java_library(
    name = 'basics-java',
    srcs = glob(["**/basics/*.java"]),
)

# config-java only depends on jdk, basics-java and yaml
# no other dependencies should be added
java_library(
    name = 'config-java',
    srcs = glob(["**/config/*.java"]),
    deps = [
        ":basics-java",
        "@org_yaml_snakeyaml//jar",
    ],
)

# network-java only depends on jdk, basics-java and proto
# no other dependencies should be added
java_library(
    name = 'network-java',
    srcs = glob(["**/network/*.java"]),
    deps = [
        ":basics-java",
        "@com_google_protobuf_protobuf_java//jar",
        "//heron/spi/src/java:utils-spi-java",
         "//third_party/java:jackson",
    ],
)

# utils-java only depends on jdk and heron-api code. No other
# dependencies should be added
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

java_library(
   name = 'test-java',
   srcs = glob(["**/test/**/*.java"]),
   deps = [
        ":basics-java",
        ":network-java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
   ]
)