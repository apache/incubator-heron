licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_binary(
    name = "kafka-mirror",
    srcs = glob(["**/*.java"]),
    main_class = "com.twitter.heron.KafkaMirrorTopology",
    deps = [
        "//heron/api/src/java:api-java-nl",
        "//contrib/kafka9/heron-kafka/src/java:heron-kafka-java"
    ],
)
