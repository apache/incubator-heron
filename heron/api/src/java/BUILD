licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

api_deps_files =  \
    heron_java_proto_files() + [
        "//3rdparty/kryo:kryo-java",
    ]

java_library(
    name = "api-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = api_deps_files,
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
<<<<<<< HEAD
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
=======
>>>>>>> master
)