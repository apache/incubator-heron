licenses(["notice"])

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "yaml-cxx",
    srcs = glob([
        "yaml-cpp-noboost/src/**/*.cpp",
        "yaml-cpp-noboost/src/**/*.h",
    ]),
    hdrs = glob([
        "yaml-cpp-noboost/include/**/*.h",
    ]),
    includes = ["yaml-cpp-noboost/include"],
    linkstatic = 1,
)
