load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "all-grouping_unittest",
    size = "small",
    srcs = [
        "all-grouping_unittest.cpp",
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
        "//heron/stmgr/src/cpp:grouping-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "custom-grouping_unittest",
    size = "small",
    srcs = [
        "custom-grouping_unittest.cpp",
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
        "//heron/stmgr/src/cpp:grouping-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "fields-grouping_unittest",
    size = "small",
    srcs = [
        "fields-grouping_unittest.cpp",
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
        "//heron/stmgr/src/cpp:grouping-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "lowest-grouping_unittest",
    size = "small",
    srcs = [
        "lowest-grouping_unittest.cpp",
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
        "//heron/stmgr/src/cpp:grouping-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "shuffle-grouping_unittest",
    size = "small",
    srcs = [
        "shuffle-grouping_unittest.cpp",
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
        "//heron/stmgr/src/cpp:grouping-cxx",
        "@com_google_googletest//:gtest",
    ],
)
