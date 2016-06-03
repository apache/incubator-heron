package(default_visibility = ["//visibility:public"])

common_deps = [
    ":util",
    "@commons_cli//jar",
    "@commons_lang//jar",
    "@guava//jar",
    "//3rdparty/java/bazel:extra_actions_proto_java",
    "//3rdparty/java/bazel:proto_java",
]

java_binary(
    name = "checkstyle_java",
    srcs = ["JavaCheckstyle.java"],
    main_class = "com.twitter.bazel.checkstyle.JavaCheckstyle",
    deps = common_deps + [
        "@checkstyle//jar",
        "//3rdparty/java:checkstyle",
    ],
)

java_binary(
    name = "checkstyle_cpp",
    srcs = ["CppCheckstyle.java"],
    main_class = "com.twitter.bazel.checkstyle.CppCheckstyle",
    deps = common_deps,
)

java_binary(
    name = "checkstyle_python",
    srcs = ["PythonCheckstyle.java"],
    main_class = "com.twitter.bazel.checkstyle.PythonCheckstyle",
    deps = common_deps,
)

java_library(
    name = "util",
    srcs = ["ExtraActionUtils.java"],
    deps = [
        "@protobuf_java//jar",
        "//3rdparty/java/bazel:extra_actions_proto_java",
        "//3rdparty/java/bazel:proto_java",
    ],
)