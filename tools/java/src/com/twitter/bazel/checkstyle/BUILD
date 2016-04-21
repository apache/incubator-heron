package(default_visibility = ["//visibility:public"])

java_binary(
    name = "checkstyle",
    srcs = glob(["*.java"]),
    main_class = "com.twitter.bazel.checkstyle.JavaCheckstyle",
    deps = [
        "@checkstyle//jar",
        "@commons-cli//jar",
        "@commons-lang//jar",
        "@guava//jar",
        "@protobuf-java//jar",
        "//3rdparty/bazel:extra_actions_proto_java",
        "//3rdparty/bazel:proto_java",
        "//3rdparty/java:checkstyle",
    ],
)