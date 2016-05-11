licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "aws-java-sdk",
    srcs = [ "Empty.java" ],
    exports = [
        "@aws-java-sdk-core//jar",
        "@aws-java-sdk-s3//jar",
    ],
    deps = [
        "@aws-java-sdk-core//jar",
        "@aws-java-sdk-s3//jar",
        "@bazel_tools//third_party:joda_time",
        "@commons-logging//jar",
        "@http-client//jar",
        "@http-core//jar",
        "@jackson-annotations//jar",
        "@jackson-core//jar",
        "@jackson-databind//jar",
    ],
)

java_library(
    name = "checkstyle",
    srcs = [ "Empty.java" ],
    exports = [ "@checkstyle//jar" ],
    deps = [
        "@antlr//jar",
        "@checkstyle//jar",
        "@commons-beanutils//jar",
        "@commons-cli//jar",
        "@commons-collections//jar",
        "@commons-logging//jar",
        "@guava//jar",
    ]
)

java_library(
    name = "jackson",
    srcs = [ "Empty.java" ],
    exports = [
        "@jackson-annotations//jar",
        "@jackson-core//jar",
        "@jackson-databind//jar",
    ],
    deps = [
        "@jackson-annotations//jar",
        "@jackson-core//jar",
        "@jackson-databind//jar",
    ],
)

java_library(
    name = "jarjar",
    srcs = [ "Empty.java" ],
    exports = [ "@jarjar//jar" ],
    deps = [
        "@asm-all//jar",
        "@jarjar//jar",
    ],
)

java_library(
    name = "logging",
    srcs = [ "Empty.java" ],
    exports = [ "@slf4j-api//jar" ],
    deps = [
        "@slf4j-api//jar",
        "@slf4j-jdk//jar",
        "@log4j-over-slf4j//jar",
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
    name = "kryo-nl",
    srcs = [ "Empty.java" ],
    exports = [ "@kryo//jar" ],
    deps = [
        "@kryo//jar",
        "@minlog//jar",
        "@objenesis//jar",
    ],
    neverlink = 1,
)

java_library(
    name = "hadoop-core",
    srcs = [ "Empty.java" ],
    exports = ["@hadoop-core//jar" ],
    deps = [
        "@hadoop-core//jar",
    ],
    neverlink = 1,
)

java_library(
    name = "powermock",
    srcs = [ "Empty.java" ],
    exports = [
        "@powermock-api-mockito//jar",
        "@powermock-core//jar",
        "@powermock-module-junit4//jar",
    ],
    deps = [
        "@powermock-api-mockito//jar",
        "@powermock-api-support//jar",
        "@powermock-core//jar",
        "@powermock-module-junit4//jar",
        "@powermock-module-junit4-common//jar",
        "@powermock-reflect//jar",
        "@javassist//jar",
    ],
)

# This version is needed for dependents that don't want
# protobuf to be included in the generated binary
java_library(
    name = "protobuf-java-neverlink",
    srcs = [ "Empty.java" ],
    deps = [ "@protobuf-java//jar" ],
    exports = [ "@checkstyle//jar" ],
    neverlink = 1,
)

java_library(
    name = "protobuf-java-nl",
    visibility = ["//visibility:public"],
    exports = [ "@protobuf-java//jar" ],
    neverlink = 1,
)