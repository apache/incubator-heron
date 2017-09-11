package(default_visibility = ["//visibility:public"])

cc_test(
    name = "rotating-map_unittest",
    srcs = [
        "rotating-map_unittest.cpp",
    ],
    deps = [
        "//heron/stmgr/src/cpp:util-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "tuple-cache_unittest",
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    srcs = [
        "tuple-cache_unittest.cpp",
    ],
    deps = [
        "//heron/stmgr/src/cpp:util-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "xor-manager_unittest",
    args = ["$(location //heron/config/src/yaml:test-config-internals-yaml)"],
    srcs = [
        "xor-manager_unittest.cpp",
    ],
    deps = [
        "//heron/stmgr/src/cpp:util-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-Iheron/stmgr/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "neighbour_calculator_unittest",
    srcs = [
        "neighbour_calculator_unittest.cpp",
    ],
    deps = [
        "//heron/stmgr/src/cpp:manager-cxx",
        "//heron/stmgr/src/cpp:grouping-cxx",
        "//heron/stmgr/src/cpp:util-cxx",
        "//third_party/gtest:gtest-cxx",
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
)
