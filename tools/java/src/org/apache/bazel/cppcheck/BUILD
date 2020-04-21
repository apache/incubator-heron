load("@rules_java//java:defs.bzl", "java_binary")

package(default_visibility = ["//visibility:public"])

common_deps = [
    "@maven//:commons_cli_commons_cli",
    "@maven//:commons_lang_commons_lang",
    "@maven//:com_google_guava_guava",
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
