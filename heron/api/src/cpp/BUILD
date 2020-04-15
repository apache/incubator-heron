load("@rules_cc//cc:defs.bzl", "cc_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "cxx-bolt-hdrs",
    srcs = glob(["bolt/*.h"]),
)

filegroup(
    name = "cxx-config-hdrs",
    srcs = glob(["config/*.h"]),
)

filegroup(
    name = "cxx-exceptions-hdrs",
    srcs = glob(["exceptions/*.h"]),
)

filegroup(
    name = "cxx-metric-hdrs",
    srcs = glob(["metric/*.h"]),
)

filegroup(
    name = "cxx-serializer-hdrs",
    srcs = glob(["serializer/*.h"]),
)

filegroup(
    name = "cxx-spout-hdrs",
    srcs = glob(["spout/*.h"]),
)

filegroup(
    name = "cxx-topology-hdrs",
    srcs = glob(["spout/*.h"]),
)

filegroup(
    name = "cxx-tuple-hdrs",
    srcs = glob(["tuple/*.h"]),
)

filegroup(
    name = "cxx-utils-hdrs",
    srcs = glob(["utils/*.h"]),
)

cc_library(
    name = "cxx-api",
    srcs = glob(["**/*.cpp"]),
    hdrs = glob(["**/*.h"]),
    copts = [
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-Iheron/api/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/proto:proto-cxx",
        "@com_github_cereal//:cereal-cxx",
    ],
)
