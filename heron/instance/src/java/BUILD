licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron_deps", "heron_java_proto_files")

instance_deps_files =  \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:common-java",
    ]

instance_deps_files_nl =  \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java-neverlink",
        "//heron/api/src/java:classification",
        "//heron/common/src/java:common-java-neverlink",
    ]

java_library(
    name = "instance-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/HeronInstance.java"],
    ),
    deps = instance_deps_files,
)

java_library(
    name = "instance-java-neverlink",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/HeronInstance.java"],
    ),
    deps = instance_deps_files_nl,
)

java_binary(
    name = "instance-unshaded",
    srcs = glob(["**/HeronInstance.java"]),
    main_class = "com.twitter.heron.instance.HeronInstance",
    deps = [
        ":instance-java",
        "//heron/common/src/java:common-java",
    ] + heron_java_proto_files(),
)

jarjar_binary(
    name = "heron-instance",
    src = ":instance-unshaded_deploy.jar",
    shade = "shade.conf",
    deps = ["@org_sonatype_plugins_jarjar_maven_plugin//jar"]
)
