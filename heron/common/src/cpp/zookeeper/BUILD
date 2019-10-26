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
        "@org_apache_zookeeper//:zookeeper",
        "//heron/common/src/cpp/network:network-cxx",
        "//heron/common/src/cpp/errors:errors-cxx",
        "//heron/common/src/cpp/basics:basics-cxx",
    ],
    linkstatic = 1,
)
