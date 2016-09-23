package(default_visibility = ["//visibility:public"])

cc_test(
    name = "stmgr_unittest",
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    srcs = [
        "dummy_instance.cpp",
        "dummy_metricsmgr.cpp",
        "dummy_stmgr.cpp",
        "stmgr_unittest.cpp",

        "dummy_instance.h",
        "dummy_metricsmgr.h",
        "dummy_stmgr.h",
    ],
    deps = [
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/stmgr/src/cpp:grouping-cxx",
        "//heron/stmgr/src/cpp:util-cxx",
        # TODO: Stmgr unit tests should not depend on tmaster
        "//heron/tmaster/src/cpp:tmaster-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-Iheron/stmgr/tests/cpp",
        "-Iheron/tmaster/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    flaky = 1,
)
