load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "metrics-cxx",
    srcs = [
        "assignable-metric.cpp",
        "assignable-metric.h",
        "count-metric.cpp",
        "count-metric.h",
        "imetric.h",
        "mean-metric.cpp",
        "mean-metric.h",
        "metrics.h",
        "metrics-mgr-st.cpp",
        "metrics-mgr-st.h",
        "metricsmgr-client.cpp",
        "metricsmgr-client.h",
        "multi-assignable-metric.cpp",
        "multi-assignable-metric.h",
        "multi-count-metric.cpp",
        "multi-count-metric.h",
        "multi-mean-metric.cpp",
        "multi-mean-metric.h",
        "time-spent-metric.cpp",
        "time-spent-metric.h",
        "tmaster-metrics.cpp",
        "tmaster-metrics.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
    ] + select({
        "//tools/platform:darwin": [],
        "//conditions:default": ["@org_nongnu_libunwind//:libunwind"],
    }),
)
