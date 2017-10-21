package(default_visibility = ["//visibility:public"])

cc_binary(
    name='exclamationTopology',
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
      "-I$(GENDIR)/third_party/protobuf/include",
      "-I$(GENDIR)/third_party/cereal/include",
    ],
    deps = [
        "//heron/api/src/cpp:cxx-api",
        "//third_party/glog:glog-cxx",
    ],
    linkstatic = 1,
)

cc_binary(
    name='libexclamationTopology.so',
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
      "-I$(GENDIR)/third_party/protobuf/include",
      "-I$(GENDIR)/third_party/cereal/include",
    ],
    deps = [
        "//heron/api/src/cpp:cxx-api",
        "//third_party/glog:glog-shared-cxx",
    ],
    linkshared = 1,
)

cc_binary(
    name='ackfailTopology',
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
      "-I$(GENDIR)/third_party/protobuf/include",
      "-I$(GENDIR)/third_party/cereal/include",
    ],
    deps = [
        "//heron/api/src/cpp:cxx-api",
        "//third_party/glog:glog-cxx",
    ],
    linkstatic = 1,
)

cc_binary(
    name='libackfailTopology.so',
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
    deps = [
        "//heron/api/src/cpp:cxx-api",
        "//third_party/glog:glog-shared-cxx",
    ],
    linkshared = 1,
)
