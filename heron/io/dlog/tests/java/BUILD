load("@rules_java//java:defs.bzl", "java_test")

common_deps_files = [
    "@maven//:org_mockito_mockito_all",
    "@maven//:junit_junit",
]

dlog_test_deps_files = \
    common_deps_files + [
        "//third_party/java:guava",
        "//heron/io/dlog/src/java:dlog-lib",
    ]

java_test(
    name = "DLInputStreamTest",
    size = "small",
    srcs = glob(["**/dlog/DLInputStreamTest.java"]),
    deps = dlog_test_deps_files,
)

java_test(
    name = "DLOutputStreamTest",
    size = "small",
    srcs = glob(["**/dlog/DLOutputStreamTest.java"]),
    deps = dlog_test_deps_files,
)
