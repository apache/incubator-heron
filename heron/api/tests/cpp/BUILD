package(default_visibility = ["//visibility:public"])

cc_test(
    name = "serialization_unittest",
    srcs = [
        "serialization_unittest.cpp",
    ],
    deps = [
        "//heron/api/src/cpp:cxx-api",
        "//heron/common/src/cpp/basics:basics-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron/api/src/cpp",
        "-Iheron/common/src/cpp",
        "-Iheron",
        "-I$(GENDIR)/heron",
    ],
    size = "small",
    linkstatic = 1,
)
