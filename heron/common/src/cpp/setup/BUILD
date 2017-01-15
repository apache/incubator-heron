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
    deps = [
        "//config:config-cxx",
        "//heron/proto:proto-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/zookeeper:zookeeper-cxx",
        "//third_party/yaml-cpp:yaml-cxx",
    ],
    linkstatic = 1,
)
