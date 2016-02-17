licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "api-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//3rdparty/kryo:kryo-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_library(
    name = "api-java-nl",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//3rdparty/kryo:kryo-java-neverlink",
        "//3rdparty/protobuf:protobuf-java-neverlink",
    ],
)