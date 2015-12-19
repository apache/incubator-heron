package(default_visibility = ["//visibility:public"])

cc_test(
    name = "heron-zkstatemgr_unittest",
    srcs = [
        "heron-zkstatemgr_unittest.cpp",
    ],
    deps = [
        "//heron/state/src/cpp:state-cxx",
        "//3rdparty/gmock:gmock-cxx",
        "//3rdparty/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/state/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)
