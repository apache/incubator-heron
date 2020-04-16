scala_test(
    name = "api-scala-test",
    srcs = glob([
        "org/apache/heron/streamlet/scala/**/*.scala",
        "org/apache/heron/resource/**/*.scala",
    ]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/scala:api-scala",
        "//third_party/java:junit4",
    ],
)
