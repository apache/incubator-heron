load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "config-cxx",
    srcs = [
        "allconfig.h",
        "cluster-config-reader.cpp",
        "cluster-config-reader.h",
        "cluster-config-vars.cpp",
        "cluster-config-vars.h",
        "helper.h",
        "heron-internals-config-reader.cpp",
        "heron-internals-config-reader.h",
        "heron-internals-config-vars.cpp",
        "heron-internals-config-vars.h",
        "metrics-sinks-reader.cpp",
        "metrics-sinks-vars.cpp",
        "metrics-sinks-vars.h",
        "operational-config-reader.cpp",
        "operational-config-reader.h",
        "operational-config-vars.cpp",
        "operational-config-vars.h",
        "physical-plan-helper.cpp",
        "physical-plan-helper.h",
        "reader.h",
        "topology-config-helper.cpp",
        "topology-config-helper.h",
        "topology-config-vars.cpp",
        "topology-config-vars.h",
        "yaml-file-reader.cpp",
    ],
    hdrs = [
        "metrics-sinks-reader.h",
        "yaml-file-reader.h",
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
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/proto:proto-cxx",
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
    ],
)
