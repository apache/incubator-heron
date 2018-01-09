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
        "@com_github_jbeder_yaml_cpp//:yaml-cxx",
    ],
    linkstatic = 1,
)
