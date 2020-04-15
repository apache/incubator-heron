load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "rotating-map_unittest",
    size = "small",
    srcs = [
        "rotating-map_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:util-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "tuple-cache_unittest",
    size = "small",
    srcs = [
        "tuple-cache_unittest.cpp",
    ],
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:util-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "xor-manager_unittest",
    size = "small",
    srcs = [
        "xor-manager_unittest.cpp",
    ],
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    linkstatic = 1,
    deps = [
        "//heron/stmgr/src/cpp:util-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "neighbour_calculator_unittest",
    srcs = [
        "neighbour_calculator_unittest.cpp",
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
