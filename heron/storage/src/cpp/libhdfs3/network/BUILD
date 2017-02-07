package(default_visibility = ["//visibility:public"])

cc_library(
    name = "network-cxx",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = [
        "-Ithird_party",
        "-Iheron/storage/src/cpp/libhdfs3",
    ],
    deps = [
        "//heron/storage/src/cpp/libhdfs3/common:common-cxx",
    ],
    linkstatic = 1,
)
