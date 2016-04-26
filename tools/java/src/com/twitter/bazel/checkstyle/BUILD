package(default_visibility = ["//visibility:public"])

java_binary(
    name = "checkstyle",
    srcs = glob(["*.java"]),
    main_class = "com.twitter.bazel.checkstyle.JavaCheckstyle",
    deps = [
        "@commons-cli//jar",
        "@commons-lang//jar",
        "@guava//jar",
        "//3rdparty/java/bazel:extra_actions_proto_java",
        "//3rdparty/java/bazel:proto_java",
        "//3rdparty/java:checkstyle",
    ],
)