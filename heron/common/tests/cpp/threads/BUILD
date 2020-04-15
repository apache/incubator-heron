load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "spcountdownlatch_unittest",
    size = "small",
    srcs = ["spcountdownlatch_unittest.cpp"],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/threads:threads-cxx",
        "@com_google_googletest//:gtest",
    ],
)
