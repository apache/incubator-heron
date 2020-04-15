load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "serialization_unittest",
    size = "small",
    srcs = [
        "serialization_unittest.cpp",
    ],
    copts = [
        "-Iheron/api/src/cpp",
        "-Iheron/common/src/cpp",
        "-Iheron",
        "-I$(GENDIR)/heron",
    ],
    linkstatic = 1,
    deps = [
        "//heron/api/src/cpp:cxx-api",
        "//heron/common/src/cpp/basics:basics-cxx",
        "@com_google_googletest//:gtest",
    ],
)
