load("@rules_cc//cc:defs.bzl", "cc_binary")

package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "exclamationTopology",
    srcs = [
        "exclamation/exclamation-topology.cpp",
        "spouts/test-word-spout.h",
    ],
    copts = [
        "-Iexamples/src/cpp",
        "-Iheron",
        "-Iheron/api/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/third_party/glog/include",
    ],
    linkstatic = 1,
    deps = [
        "//heron/api/src/cpp:cxx-api",
    ],
)

cc_binary(
    name = "libexclamationTopology.so",
    srcs = [
        "exclamation/exclamation-topology.cpp",
        "spouts/test-word-spout.h",
    ],
    copts = [
        "-Iexamples/src/cpp",
        "-Iheron/api/src/cpp",
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/third_party/glog/include",
    ],
    linkstatic = 1,
    deps = [
        "//heron/api/src/cpp:cxx-api",
    ],
)

cc_binary(
    name = "ackfailTopology",
    srcs = [
        "ackfail/ackfail-topology.cpp",
        "spouts/test-word-spout.h",
    ],
    copts = [
        "-Iexamples/src/cpp",
        "-Iheron/api/src/cpp",
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/third_party/glog/include",
    ],
    linkstatic = 1,
    deps = [
        "//heron/api/src/cpp:cxx-api",
    ],
)

cc_binary(
    name = "libackfailTopology.so",
    srcs = [
        "ackfail/ackfail-topology.cpp",
        "spouts/test-word-spout.h",
    ],
    copts = [
        "-Iexamples/src/cpp",
        "-Iheron/api/src/cpp",
        "-Iheron",
        "-I$(GENDIR)/heron",
    ],
    linkstatic = 1,
    deps = [
        "//heron/api/src/cpp:cxx-api",
    ],
)
