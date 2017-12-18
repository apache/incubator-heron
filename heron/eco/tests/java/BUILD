test_deps_files = [
    "//third_party/java:powermock",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "@com_google_guava_guava//jar",
]

heron_local_deps = [
    "//heron/eco/src/java:eco-java",
    "//heron/api/src/java:api-java-low-level",
]

eco_test_deps = heron_local_deps + test_deps_files




java_test(
    name = "EcoBuilderTest",
    srcs = glob(["com/twitter/heron/eco/helper/EcoBuilderTest.java"]),
    deps = eco_test_deps,
    size = "small",
)