load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "zookeeper-cxx",
    srcs = [
        "zkclient.cpp",
        "zkclient_factory.h",
    ],
    hdrs = [
        "mock_zkclient.h",
        "zkclient.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron//common/src/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/basics:basics-cxx",
        "//heron/common/src/cpp/errors:errors-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "@org_apache_zookeeper//:zookeeper",
    ],
)
