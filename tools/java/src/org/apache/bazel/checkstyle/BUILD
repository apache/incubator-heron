load("@rules_java//java:defs.bzl", "java_binary", "java_library")

package(default_visibility = ["//visibility:public"])

common_deps = [
    ":util",
    "@maven//:commons_cli_commons_cli",
    "@maven//:commons_lang_commons_lang",
    "@maven//:com_google_guava_guava",
    "//third_party/java/bazel:extra_actions_proto_java",
    "//third_party/java/bazel:proto_java",
]

java_binary(
    name = "checkstyle_java",
    srcs = ["JavaCheckstyle.java"],
    main_class = "org.apache.bazel.checkstyle.JavaCheckstyle",
    deps = common_deps + [
        "@maven//:com_puppycrawl_tools_checkstyle",
        "//third_party/java:checkstyle",
    ],
)

java_binary(
    name = "checkstyle_cpp",
    srcs = ["CppCheckstyle.java"],
    main_class = "org.apache.bazel.checkstyle.CppCheckstyle",
    deps = common_deps,
)

java_binary(
    name = "checkstyle_python",
    srcs = ["PythonCheckstyle.java"],
    main_class = "org.apache.bazel.checkstyle.PythonCheckstyle",
    deps = common_deps,
)

java_library(
    name = "util",
    srcs = ["ExtraActionUtils.java"],
    deps = [
        "//third_party/java/bazel:extra_actions_proto_java",
        "//third_party/java/bazel:proto_java",
        "@com_google_protobuf//:protobuf_java",
    ],
)
