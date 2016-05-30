licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "aws-java-sdk",
    srcs = [ "Empty.java" ],
    exports = [
        "@aws_java_sdk_core//jar",
        "@aws_java_sdk_s3//jar",
    ],
    deps = [
        "@aws_java_sdk_core//jar",
        "@aws_java_sdk_s3//jar",
        "@bazel_tools//third_party:joda_time",
        "@commons_logging//jar",
        "@http_client//jar",
        "@http_core//jar",
        "@jackson_annotations//jar",
        "@jackson_core//jar",
        "@jackson_databind//jar",
    ],
)

java_library(
    name = "checkstyle",
    srcs = [ "Empty.java" ],
    exports = [ "@checkstyle//jar" ],
    deps = [
        "@antlr//jar",
        "@checkstyle//jar",
        "@commons_beanutils//jar",
        "@commons_cli//jar",
        "@commons_collections//jar",
        "@commons_logging//jar",
        "@guava//jar",
    ]
)

java_library(
    name = "jackson",
    srcs = [ "Empty.java" ],
    exports = [
        "@jackson_annotations//jar",
        "@jackson_core//jar",
        "@jackson_databind//jar",
    ],
    deps = [
        "@jackson_annotations//jar",
        "@jackson_core//jar",
        "@jackson_databind//jar",
    ],
)

java_library(
    name = "jarjar",
    srcs = [ "Empty.java" ],
    exports = [ "@jarjar//jar" ],
    deps = [
        "@asm_all//jar",
        "@jarjar//jar",
    ],
)

java_library(
    name = "logging",
    srcs = [ "Empty.java" ],
    exports = [ "@slf4j_api//jar" ],
    deps = [
        "@slf4j_api//jar",
        "@slf4j_jdk//jar",
        "@log4j_over_slf4j//jar",
    ],
)

java_library(
    name = "kryo",
    srcs = [ "Empty.java" ],
    exports = [ "@kryo//jar" ],
    deps = [
        "@kryo//jar",
        "@minlog//jar",
        "@objenesis//jar",
    ],
)

java_library(
    name = "hadoop-core",
    srcs = [ "Empty.java" ],
    exports = ["@hadoop_core//jar" ],
    deps = [
        "@hadoop_core//jar",
    ],
    neverlink = 1,
)

java_library(
    name = "powermock",
    srcs = [ "Empty.java" ],
    exports = [
        "@powermock_api_mockito//jar",
        "@powermock_core//jar",
        "@powermock_module_junit4//jar",
    ],
    deps = [
        "@powermock_api_mockito//jar",
        "@powermock_api_support//jar",
        "@powermock_core//jar",
        "@powermock_module_junit4//jar",
        "@powermock_module_junit4-common//jar",
        "@powermock_reflect//jar",
        "@javassist//jar",
    ],
)

# This version is needed for dependents that don't want
# protobuf to be included in the generated binary
java_library(
    name = "protobuf-java-neverlink",
    srcs = [ "Empty.java" ],
    deps = [ "@protobuf_java//jar" ],
    exports = [ "@checkstyle//jar" ],
    neverlink = 1,
)
