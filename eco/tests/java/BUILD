test_deps_files = [
    "//third_party/java:powermock",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "@org_yaml_snakeyaml//jar",
]

heron_local_deps = [
    "//eco/src/java:eco-java",
    "//heron/api/src/java:api-java-low-level",
    "//storm-compatibility/src/java:storm-compatibility-java",
]

eco_test_deps = heron_local_deps + test_deps_files

java_test(
    name = "EcoBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/EcoBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small",
)

java_test(
    name = "EcoParserTest",
    srcs = glob(["org/apache/heron/eco/parser/EcoParserTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "ConfigBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/ConfigBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "BoltBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/BoltBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "ComponentBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/ComponentBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "SpoutBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/SpoutBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "StreamBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/StreamBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "EcoTest",
    srcs = glob(["org/apache/heron/eco/EcoTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "ObjectBuilderTest",
    srcs = glob(["org/apache/heron/eco/builder/ObjectBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "BuilderUtilityTest",
    srcs = glob(["org/apache/heron/eco/builder/BuilderUtilityTest.java"]),
    deps = eco_test_deps,
    size = "small"
)

java_test(
    name = "EcoSubmitterTest",
    srcs = glob(["org/apache/heron/eco/submit/EcoSubmitterTest.java"]),
    deps = eco_test_deps,
    size = "small"
)
