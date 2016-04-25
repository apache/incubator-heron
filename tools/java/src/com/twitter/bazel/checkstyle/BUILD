package(default_visibility = ["//visibility:public"])

java_binary(
    name = "checkstyle_java",
    srcs = ["JavaCheckstyle.java"],
    main_class = "com.twitter.bazel.checkstyle.JavaCheckstyle",
    deps = [
        ":util",
        "@checkstyle//jar",
        "@commons-cli//jar",
        "@commons-lang//jar",
        "@guava//jar",
        "//3rdparty/bazel:extra_actions_proto_java",
        "//3rdparty/bazel:proto_java",
        "//3rdparty/java:checkstyle",
    ],
)

java_binary(
    name = "checkstyle_cpp",
    srcs = ["CppCheckstyle.java"],
    main_class = "com.twitter.bazel.checkstyle.CppCheckstyle",
    deps = [
        ":util",
        "@commons-cli//jar",
        "@commons-lang//jar",
        "@guava//jar",
        "//3rdparty/bazel:extra_actions_proto_java",
        "//3rdparty/bazel:proto_java",
    ],
)

java_library(
    name = "util",
    srcs = ["ExtraActionUtils.java"],
    deps = [
        "@protobuf-java//jar",
        "//3rdparty/bazel:extra_actions_proto_java",
        "//3rdparty/bazel:proto_java",
    ],
)