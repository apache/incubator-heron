package(default_visibility = ["//visibility:public"])

cc_test(
    name = "tmaster_unittest",
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    srcs = [
        "dummystmgr.cpp",
        "tmaster_unittest.cpp",
        "dummystmgr.h",
    ],
    deps = [
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/tmaster/src/cpp:tmaster-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    data = [
        "//heron/config/src/yaml:test-config-internals-yaml",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-Iheron/tmaster/src/cpp",
        "-Iheron/tmaster/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    size = "small",
    flaky = 1,
    linkstatic = 1,
)
