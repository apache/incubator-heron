package(default_visibility = ["//visibility:public"])

common_deps = [
    "@commons_cli_commons_cli//jar",
    "@commons_lang_commons_lang//jar",
    "@com_google_guava_guava//jar",
    "//third_party/java/bazel:extra_actions_proto_java",
    "//third_party/java/bazel:proto_java",
    "//tools/java/src/org/apache/bazel/checkstyle:util",
]

java_binary(
    name = "cppcheck_cpp",
    srcs = ["CppCheck.java"],
    main_class = "org.apache.bazel.cppcheck.CppCheck",
    deps = common_deps,
)
