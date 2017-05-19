licenses(["notice"])

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "cxx-api",
    srcs = glob(["**/*.cpp"]),
    hdrs = glob(["**/*.h"]),
    copts = [
        "-Iheron",
        "-I$(GENDIR)/heron",
        "-Iheron/api/src/cpp",
    ],
    deps = [
        "//heron/proto:proto-cxx",
        "//third_party/cereal:cereal-cxx"
    ],
    linkstatic = 1,
)
