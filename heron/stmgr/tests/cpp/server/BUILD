load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "stmgr_unittest",
    srcs = [
        "dummy_instance.cpp",
        "dummy_instance.h",
        "dummy_metricsmgr.cpp",
        "dummy_metricsmgr.h",
        "dummy_stmgr.cpp",
        "dummy_stmgr.h",
        "stmgr_unittest.cpp",
    ],
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
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
    data = [
        "//heron/config/src/yaml:test-config-internals-yaml",
        "//heron/config/src/yaml:test-config-metrics-sinks-yaml",
    ],
    flaky = 1,
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/stmgr/src/cpp:grouping-cxx",
        "//heron/stmgr/src/cpp:util-cxx",
        # TODO: Stmgr unit tests should not depend on tmaster
        "//heron/tmaster/src/cpp:tmaster-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "checkpoint-gateway_unittest",
    srcs = [
        "checkpoint-gateway_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-Iheron/stmgr/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:grouping-cxx",
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/stmgr/src/cpp:util-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "stateful-restorer_unittest",
    srcs = [
        "dummy_ckptmgr_client.cpp",
        "dummy_ckptmgr_client.h",
        "dummy_instance_server.h",
        "dummy_stmgr_clientmgr.h",
        "dummy_tuple_cache.h",
        "stateful-restorer_unittest.cpp",
    ],
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/statemgrs/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-Iheron/stmgr/tests/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:grouping-cxx",
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/stmgr/src/cpp:util-cxx",
        "@com_google_googletest//:gtest",
    ],
)
