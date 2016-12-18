package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common-cxx",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = [
        "-Ithird_party",
        "-Iheron/storage/src/cpp/libhdfs3",
    ],
    deps = [
        "//config:config-cxx",
        "//third_party/gsasl:gsasl-cxx",
        "//third_party/libxml2:libxml2-cxx",
        "//third_party/protobuf:protobuf-cxx",
    ],
    linkstatic = 1,
)
