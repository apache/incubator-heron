package(default_visibility = ["//visibility:public"])

load("//tools/rules/pex:pex_rules.bzl", "pex_binary", "pex_library", "pex_pytest")

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
