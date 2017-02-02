package(default_visibility = ["//visibility:public"])

cc_library(
    name = "checkpointmgr-cxx",
    srcs = [
        "checkpointmgr-client.cpp",

        "checkpointmgr-client.h",
    ],
    copts = [
        "-Iheron",
        "-Iheron/common/src/cpp",
        "-I$(GENDIR)/heron",
        "-I$(GENDIR)/heron/common/src/cpp",
    ],
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/config:config-cxx",
        "//third_party/glog:glog-cxx",
        "//third_party/protobuf:protobuf-cxx",
    ],
    linkstatic = 1,
)
