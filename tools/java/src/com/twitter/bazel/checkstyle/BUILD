package(default_visibility = ["//visibility:public"])

common_deps = [
    ":util",
    "@commons_cli_commons_cli//jar",
    "@commons_lang_commons_lang//jar",
    "@com_google_guava_guava//jar",
    "//third_party/java/bazel:extra_actions_proto_java",
    "//third_party/java/bazel:proto_java",
]

java_binary(
    name = "checkstyle_java",
    srcs = ["JavaCheckstyle.java"],
    main_class = "com.twitter.bazel.checkstyle.JavaCheckstyle",
    deps = common_deps + [
        "@com_puppycrawl_tools_checkstyle//jar",
        "//third_party/java:checkstyle",
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
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java/bazel:extra_actions_proto_java",
        "//third_party/java/bazel:proto_java",
    ],
)
