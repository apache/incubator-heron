java_library(
    name = "heron-kafka-common-java",
    srcs = glob([
        "com/twitter/heron/bolts/kafka/**/*.java",
        "com/twitter/heron/storage/**/*.java",
        "com/twitter/heron/spouts/kafka/common/**/*.java",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "@json-simple//jar",
        "@gson-java//jar",
        "@slf4j-api//jar",
        "@curator-framework//jar",
        "@curator-client//jar",
        "@guava//jar",
        "@zookeeper//jar",
        "//heron/api/src/java:api-java-nl",
        "//3rdparty/java:kafka-clients-09-java-nl",
    ],
)

java_library(
    name = "heron-kafka-08-java",
    srcs = glob(["com/twitter/heron/spouts/kafka/old/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "@json-simple//jar",
        "@gson-java//jar",
        "@slf4j-api//jar",
        "@curator-framework//jar",
        "@curator-client//jar",
        "@guava//jar",
        "@zookeeper//jar",
        "//heron/api/src/java:api-java-nl",
        "//contrib/kafka9/heron-kafka/src/java:heron-kafka-common-java",
        "@scala-library-java//jar",
        "@kafka-clients-08-java//jar",
        "@kafka-08-java//jar",
        "@metrics-core-java//jar",
    ],
)

java_library(
    name = "heron-kafka-09-java",
    srcs = glob(
        ["com/twitter/heron/spouts/kafka/**/*.java"],
        exclude = [
            "com/twitter/heron/spouts/kafka/common/**/*.java",
            "com/twitter/heron/spouts/kafka/old/**/*.java",
        ],
    ),
    visibility = ["//visibility:public"],
    deps = [
        "@json-simple//jar",
        "@gson-java//jar",
        "@slf4j-api//jar",
        "@curator-framework//jar",
        "@curator-client//jar",
        "@guava//jar",
        "@zookeeper//jar",
        "//heron/api/src/java:api-java-nl",
        "//contrib/kafka9/heron-kafka/src/java:heron-kafka-common-java",
        "@scala-library-java//jar",
        "@kafka-clients-09-java//jar",
        "@kafka-09-java//jar",
        "@metrics-core-java//jar",
    ],
)
