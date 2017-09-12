package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/build_defs", "DOCLINT_HTML_AND_SYNTAX")
load("/tools/rules/javadoc", "java_doc")

storm_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:basics-java",
    "//heron/simulator/src/java:simulator-java",
    "//heron/proto:proto_topology_java",
    "@com_googlecode_json_simple_json_simple//jar",
    "//heron/proto:proto_topology_java",  # added for trident prototype
    "@commons_io_commons_io//jar",        # added for trident prototype
    "@commons_lang_commons_lang//jar",    # added for trident prototype
    "@org_apache_storm_core//jar",        # added for trident prototype
    "@org_clojure_clojure//jar",          # added for trident prototype
    "@org_slf4j_slf4j_api//jar",          # added for trident prototype
    "@org_slf4j_slf4j_jdk14//jar",          # added for trident prototype
    "@org_yaml_snakeyaml//jar",           # added for trident prototype
    "@org_ow2_asm_asm_all//jar",          # added for trident prototype, required at runtime
    "//third_party/java:kryo-neverlink",
]

java_doc(
    name = "heron-storm-javadoc",
    libs = [":storm-compatibility-java"],
    pkgs = ["backtype", "clojure", "org", "storm"],
    title = "Heron Storm Compatibility Api Documentation",
)

# Kryo is bundled here for integration test
java_library(
    name='storm-compatibility-java',
    srcs = glob(["**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = storm_deps_files + ["//third_party/java:kryo"],
)

java_binary(
    name="storm-compatibility-unshaded",
    srcs = glob(["**/*.java"]),
    deps = storm_deps_files,
)

jarjar_binary(
    name = "heron-storm",
    src = ":storm-compatibility-unshaded_deploy.jar",
    shade = "shade.conf",
    deps = ["@org_sonatype_plugins_jarjar_maven_plugin//jar"]
)
