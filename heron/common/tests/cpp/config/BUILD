package(default_visibility = ["//visibility:public"])

cc_test(
    name = "config_unittest",
    srcs = ["config_unittest.cpp"],
    deps = [
        "//heron/common/src/cpp/config:config-cxx",
        "//heron/common/src/cpp/basics:basics-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-I.",
        "-Iheron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)
