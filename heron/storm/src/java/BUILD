package(default_visibility = ["//visibility:public"])

storm_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/localmode/src/java:localmode-java",
    "//3rdparty/json-simple:json-simple-java",
    "//3rdparty/kryo:kryo-java",
]

java_library(
    name='storm-compatibility-java',
    srcs = glob(["**/*.java"]),
    deps = storm_deps_files, 
)

java_binary(
    name = "storm-compatibility-unshaded",
    srcs = glob(["**/*.java"]),
    deps = storm_deps_files,
)

genrule(
    name = "heron-storm",
    srcs = [":storm-compatibility-unshaded_deploy.jar"],
    outs = ["heron-storm.jar"],
    cmd  = "cp $< $@",
)
