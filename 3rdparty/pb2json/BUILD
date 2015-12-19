licenses(["notice"])

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "pb2json-cxx",
    srcs = [
        "pb2json.cpp",
    ],
    hdrs = [ 
        "pb2json.h",
    ],
    deps = [
        "//3rdparty/jansson:jansson-cxx",
        "//3rdparty/protobuf:protobuf-cxx",
    ],
    linkstatic = 1,
)
