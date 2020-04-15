load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "count-metric_unittest",
    size = "small",
    srcs = [
        "count-metric_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "mean-metric_unittest",
    size = "small",
    srcs = [
        "mean-metric_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "multi-count-metric_unittest",
    size = "small",
    srcs = [
        "multi-count-metric_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "multi-mean-metric_unittest",
    size = "small",
    srcs = [
        "multi-mean-metric_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "time-spent-metric_unittest",
    size = "small",
    srcs = [
        "time-spent-metric_unittest.cpp",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/metrics:metrics-cxx",
        "@com_google_googletest//:gtest",
    ],
)
