test_deps_files = [
    "//third_party/java:powermock",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
]

heron_local_deps = [
    "//heron/eco/src/java:eco-java",
    "//heron/api/src/java:api-java-low-level",
    "//storm-compatibility/src/java:storm-compatibility-java",
]

eco_test_deps = heron_local_deps + test_deps_files




java_test(
    name = "EcoBuilderTest",
    srcs = glob(["com/twitter/heron/eco/builder/EcoBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small",
)

java_test(
    name = "EcoParserTest",
    srcs = glob(["com/twitter/heron/eco/parser/EcoParserTest.java"]),
    deps = eco_test_deps,
    size = "small"
)