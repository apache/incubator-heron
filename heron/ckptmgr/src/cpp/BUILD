package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common-cxx",
    srcs = glob(["common/*.cpp"]),
    hdrs = glob(["common/*.h"]),
    copts = [
        "-Ithird_party",
        "-Iheron/ckptmgr/src/cpp",
    ],
    linkstatic = 1,
)
