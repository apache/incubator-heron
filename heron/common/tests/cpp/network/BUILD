load("//tools/rules:genproto.bzl", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "proto_unittests",
    src = "unittests.proto",
    gen_cc = 1,
    includes = ["heron/common/tests/cpp/network"],
)

cc_test(
    name = "http_unittest",
    size = "small",
    srcs = [
        "host_unittest.h",
        "http_client_unittest.cpp",
        "http_server_unittest.cpp",
        "http_server_unittest.h",
        "http_unittest.cpp",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/network:network-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "order_unittest",
    size = "small",
    srcs = [
        "host_unittest.h",
        "oclient_unittest.cpp",
        "oclient_unittest.h",
        "order_unittest.cpp",
        "oserver_unittest.cpp",
        "oserver_unittest.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "packet_unittest",
    size = "small",
    srcs = [
        "packet_unittest.cpp",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "switch_unittest",
    size = "small",
    srcs = [
        "client_unittest.cpp",
        "client_unittest.h",
        "host_unittest.h",
        "server_unittest.cpp",
        "server_unittest.h",
        "switch_unittest.cpp",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "piper_unittest",
    size = "small",
    srcs = [
        "piper_unittest.cpp",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    linkstatic = 1,
    deps = [
        "//heron/common/src/cpp/network:network-cxx",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "rate_limit_unittest",
    size = "small",
    srcs = [
        "client_unittest.cpp",
        "client_unittest.h",
        "host_unittest.h",
        "rate_limit_unittest.cpp",
        "server_unittest.cpp",
        "server_unittest.h",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    linkstatic = 1,
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "@com_google_googletest//:gtest",
    ],
)
