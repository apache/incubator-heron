package(default_visibility = ["//visibility:public"])

load("/tools/rules/genproto", "proto_library")

proto_library(
    name = "proto_unittests",
    src = "unittests.proto",
    includes = ["heron/common/tests/cpp/network"],
    gen_cc = 1,
)

cc_test(
    name = "http_unittest",
    srcs = [
        "http_unittest.cpp",
        "http_client_unittest.cpp",
        "http_server_unittest.cpp",

        "host_unittest.h",
        "http_server_unittest.h",
    ],
    deps = [
        "//heron/common/src/cpp/network:network-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "order_unittest",
    srcs = [
        "order_unittest.cpp",
        "oclient_unittest.cpp",
        "oserver_unittest.cpp",

        "host_unittest.h",
        "oclient_unittest.h",
        "oserver_unittest.h",
    ],
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "packet_unittest",
    srcs = [
        "packet_unittest.cpp",
    ],
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)

cc_test(
    name = "switch_unittest",
    srcs = [
        "switch_unittest.cpp",
        "client_unittest.cpp",
        "server_unittest.cpp",

        "client_unittest.h",
        "server_unittest.h",
        "host_unittest.h",
    ],
    deps = [
        ":proto_unittests_cc",
        "//heron/common/src/cpp/network:network-cxx",
        "//third_party/gtest:gtest-cxx",
    ],
    copts = [
        "-Iheron/common/src/cpp",
        "-Iheron/common/tests/cpp",
        "-I$(GENDIR)/heron/common/src/cpp",
        "-I$(GENDIR)/heron/common/tests/cpp",
    ],
    size = "small",
    linkstatic = 1,
)
