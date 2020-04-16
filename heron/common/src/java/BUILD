load("@rules_java//java:defs.bzl", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

# The heron/common targets is divided into 3 levels by design:
# 1. only depends on jdk;
# 2. depends on jdk and heron internal code;
# 3. depends on jdk, heron internal code and 3rdparty code.

# basics-java only depends on jdk.
# No other heron code and 3rdparty dependencies should be added
java_library(
    name = "basics-java",
    srcs = glob(["**/basics/*.java"]),
)

# config-java only depends on jdk, basics-java and yaml
# no other dependencies should be added
java_library(
    name = "config-java",
    srcs = glob(["**/config/*.java"]),
    deps = [
        ":basics-java",
        "@maven//:javax_xml_bind_jaxb_api",
        "@maven//:org_yaml_snakeyaml",
    ],
)

# network-java only depends on jdk, basics-java and proto
# no other dependencies should be added
java_library(
    name = "network-java",
    srcs = glob(["**/network/*.java"]),
    deps = [
        ":basics-java",
        "@com_google_protobuf//:protobuf_java",
    ],
)

# utils-java only depends on jdk and heron-api code. No other
# dependencies should be added
java_library(
    name = "utils-java",
    srcs = glob(["**/utils/**/*.java"]),
    deps = heron_java_proto_files() + [
        ":basics-java",
        ":config-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/java:classification",
    ],
)

java_library(
    name = "test-helpers-java",
    srcs = glob(["**/testhelpers/**/*.java"]),
    deps = [
        ":basics-java",
        ":network-java",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "@com_google_protobuf//:protobuf_java",
    ],
)
