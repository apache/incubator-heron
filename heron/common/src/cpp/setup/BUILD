load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "setup-cxx",
    srcs = [
        "zk-setup.cpp",
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
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//heron/proto:proto-cxx",
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
    ],
)
