load("@rules_java//java:defs.bzl", "java_library")

common_deps_files = [
    "//third_party/java:powermock",
    "@maven//:commons_io_commons_io",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
]

apiserver_test_deps_files = \
    common_deps_files + [
        "//heron/tools/apiserver/src/java:heron-apiserver",
    ]

java_library(
    name = "tests",
    srcs = glob(["**/apiserver/**/*.java"]),
    deps = apiserver_test_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.apiserver.utils.ConfigUtilsTests",
        "org.apache.heron.apiserver.resources.TopologyResourceTests",
    ],
    runtime_deps = [":tests"],
)
