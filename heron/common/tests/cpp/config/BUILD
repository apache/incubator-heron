load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "topology-config-helper_unittest",
    size = "small",
    srcs = ["topology-config-helper_unittest.cpp"],
    copts = [
        "-I.",
        "-I$(GENDIR)/heron",
        "-Iheron",
        "-Iheron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/config:config-cxx",
        "@com_google_googletest//:gtest",
    ],
)
