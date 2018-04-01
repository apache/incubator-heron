package(default_visibility = ["//visibility:public"])

cc_test(
    name = "spcountdownlatch_unittest",
    srcs = ["spcountdownlatch_unittest.cpp"],
    deps = [
        "//heron/common/src/cpp/threads:threads-cxx",
        "@com_google_googletest//:gtest",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)
