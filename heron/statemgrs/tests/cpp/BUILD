load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "zk-statemgr_unittest",
    size = "small",
    srcs = [
        "zkstatemgr_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/statemgrs/src/cpp:statemgrs-cxx",
        "@com_google_googletest//:gtest",
    ],
)
