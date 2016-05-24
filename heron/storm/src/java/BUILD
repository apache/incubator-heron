package(default_visibility = ["//visibility:public"])

storm_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
    "//heron/simulator/src/java:simulator-java",
    "@json-simple//jar",
    "//3rdparty/java:kryo",
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
