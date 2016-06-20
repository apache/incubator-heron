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
    deps = [
        "//third_party/zookeeper:zookeeper-cxx",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/errors:errors-cxx",
        "//heron/common/src/cpp/basics:basics-cxx",
    ],
    linkstatic = 1,
)
