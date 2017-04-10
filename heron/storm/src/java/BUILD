package(default_visibility = ["//visibility:public"])

storm_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
    "//heron/simulator/src/java:simulator-java",
    "@com_googlecode_json_simple_json_simple//jar",
    "//heron/proto:proto_topology_java",  # added for trident prototype
    "@commons_io_commons_io//jar",        # added for trident prototype
    "@commons_lang_commons_lang//jar",    # added for trident prototype
    "@org_apache_storm_core//jar",        # added for trident prototype
    "@org_clojure_clojure//jar",          # added for trident prototype
    "@org_yaml_snakeyaml//jar",           # added for trident prototype
    "@org_ow2_asm_asm_all//jar",          # added for trident prototype, required at runtime
    "//third_party/java:kryo",
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
