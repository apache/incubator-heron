load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "fileutils_unittest",
    size = "small",
    srcs = ["fileutils_unittest.cpp"],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "rid_unittest",
    size = "small",
    srcs = ["rid_unittest.cpp"],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "strutils_unittest",
    size = "small",
    srcs = ["strutils_unittest.cpp"],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "utils_unittest",
    size = "small",
    srcs = ["utils_unittest.cpp"],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
        "@com_google_googletest//:gtest",
    ],
)
