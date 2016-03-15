java_library(
    name = "heron-kafka-common-java",
    srcs = glob([
        "com/twitter/heron/bolts/kafka/**/*.java",
        "com/twitter/heron/storage/**/*.java",
        "com/twitter/heron/spouts/kafka/common/**/*.java",
    ]),
    visibility = ["//visibility:public"],
    deps = [
        "//3rdparty/json-simple:json-simple-java",
        "//3rdparty/gson:gson-java",
        "//3rdparty/logging:slf4j-api-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/zookeeper:zookeeper-java",
        "//heron/api/src/java:api-java-nl",
        "//3rdparty/kafka:kafka-clients-09-java-nl",
    ],
)

java_library(
    name = "heron-kafka-08-java",
    srcs = glob(["com/twitter/heron/spouts/kafka/old/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//3rdparty/json-simple:json-simple-java",
        "//3rdparty/gson:gson-java",
        "//3rdparty/logging:slf4j-api-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/zookeeper:zookeeper-java",
        "//heron/api/src/java:api-java-nl",
        "//contrib/kafka9/heron-kafka/src/java:heron-kafka-common-java",
        "//3rdparty/scala:scala-java",
        "//3rdparty/kafka:kafka-clients-08-java",
        "//3rdparty/kafka:kafka-08-java",
        "//3rdparty/yammer:metrics-core-java",
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
        "//3rdparty/json-simple:json-simple-java",
        "//3rdparty/gson:gson-java",
        "//3rdparty/logging:slf4j-api-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/zookeeper:zookeeper-java",
        "//heron/api/src/java:api-java-nl",
        "//contrib/kafka9/heron-kafka/src/java:heron-kafka-common-java",
        "//3rdparty/scala:scala-java",
        "//3rdparty/kafka:kafka-clients-09-java",
        "//3rdparty/kafka:kafka-09-java",
        "//3rdparty/yammer:metrics-core-java",
    ],
)
