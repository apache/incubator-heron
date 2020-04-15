load("@rules_java//java:defs.bzl", "java_test")

test_deps_files = [
    "//third_party/java:powermock",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "@maven//:org_yaml_snakeyaml",
]

heron_local_deps = [
    "//eco/src/java:eco-java",
    "//eco/src/java:eco-submit-java",
    "//eco/src/java:eco-builder-java",
    "//eco/src/java:eco-defs-java",
    "//eco/src/java:eco-parser-java",
    "//eco/src/java:eco-storm-builder-java",
    "//eco/src/java:eco-heron-builder-java",
    "//heron/api/src/java:api-java-low-level",
    "//storm-compatibility/src/java:storm-compatibility-java",
]

eco_test_deps = heron_local_deps + test_deps_files

java_test(
    name = "StormEcoBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/storm/StormEcoBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "HeronEcoBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/heron/HeronEcoBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "EcoParserTest",
    size = "small",
    srcs = ["org/apache/heron/eco/parser/EcoParserTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "ConfigBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/ConfigBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "BoltBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/BoltBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "ComponentBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/ComponentBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "StormSpoutBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/storm/StormSpoutBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "HeronSpoutBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/heron/HeronSpoutBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "StormStreamBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/storm/StormStreamBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "HeronStreamBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/heron/HeronStreamBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "EcoTest",
    size = "small",
    srcs = ["org/apache/heron/eco/EcoTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "ObjectBuilderTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/ObjectBuilderTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "BuilderUtilityTest",
    size = "small",
    srcs = ["org/apache/heron/eco/builder/BuilderUtilityTest.java"],
    deps = eco_test_deps,
)

java_test(
    name = "EcoSubmitterTest",
    size = "small",
    srcs = ["org/apache/heron/eco/submit/EcoSubmitterTest.java"],
    deps = eco_test_deps,
)
