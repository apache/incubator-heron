package(default_visibility = ["//visibility:public"])

cc_test(
    name = "topology-config-helper_unittest",
    srcs = ["topology-config-helper_unittest.cpp"],
    deps = [
        "//heron/common/src/cpp/config:config-cxx",
        "@com_google_googletest//:gtest",
    ],
    copts = [
        "-I.",
        "-I$(GENDIR)/heron",
        "-Iheron",
        "-Iheron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)
