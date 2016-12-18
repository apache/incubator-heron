package(default_visibility = ["//visibility:public"])

cc_library(
    name = "rpc-cxx",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = [
        "-Ithird_party",
        "-Iheron/storage/src/cpp/libhdfs3",
        "-I$(GENDIR)/heron/storage/src/cpp/libhdfs3",
    ],
    deps = [
        "//heron/storage/src/cpp/libhdfs3/network:network-cxx",
        "//heron/storage/src/cpp/libhdfs3/common:common-cxx",
        "//heron/storage/src/cpp/libhdfs3/proto:proto-cxx",
    ],
    linkstatic = 1,
)
