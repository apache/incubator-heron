package(default_visibility = ["//visibility:public"])

cc_test(
    name = "checkpoint_unittest",
    srcs = ["checkpoint_unittest.cpp"],
    copts = [
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-Iheron/ckptmgr/src/cpp",
    ],
    deps = [
        "//heron/ckptmgr/src/cpp:common-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    size = "small",
    linkstatic = 1,
)
