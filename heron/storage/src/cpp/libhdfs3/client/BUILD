package(default_visibility = ["//visibility:public"])

cc_library(
    name = "client-cxx",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = [
        "-Ithird_party",
        "-Iheron/storage/src/cpp/libhdfs3",
        "-I$(GENDIR)/heron/storage/src/cpp/libhdfs3",
    ],
    deps = [
        "//heron/storage/src/cpp/libhdfs3/server:server-cxx",
    ],
    linkstatic = 1,
)
