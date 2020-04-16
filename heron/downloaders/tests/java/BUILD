load("@rules_java//java:defs.bzl", "java_library", "java_test")

common_deps_files = [
    "//third_party/java:powermock",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
    "//third_party/java:commons-compress",
    "//heron/common/src/java:basics-java",
]

downloader_test_deps_files = \
    common_deps_files + [
        "//heron/downloaders/src/java:heron-downloader",
    ]

java_library(
    name = "tests",
    srcs = glob(["**/downloader/**/*.java"]),
    deps = downloader_test_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.downloader.ExtractorTests",
    ],
    runtime_deps = [":tests"],
)

java_test(
    name = "RegistryTest",
    size = "small",
    srcs = glob(["**/downloader/RegistryTest.java"]),
    deps = downloader_test_deps_files,
)

java_test(
    name = "DLDownloaderTest",
    size = "small",
    srcs = glob(["**/downloader/DLDownloaderTest.java"]),
    deps = downloader_test_deps_files,
)
