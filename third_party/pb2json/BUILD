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
        "//third_party/jansson:jansson-cxx",
        "//third_party/protobuf:protobuf-cxx",
    ],
    linkstatic = 1,
)
