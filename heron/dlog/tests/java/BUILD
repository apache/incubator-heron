load("/tools/rules/java_tests", "java_tests")

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//third_party/java:mockito",
    "//third_party/java:junit4"
]

dlog_test_deps_files = \
  common_deps_files + [
    "//heron/dlog/src/java:dlog-lib"
  ]
  
java_library(
    name = "tests",
    srcs = glob(["**/dlog/**/*.java"]),
    deps = dlog_test_deps_files,
)

java_tests(
    test_classes = [
        "com.twitter.heron.dlog.DLInputStreamTest",
    ],
    runtime_deps = [ ":tests" ],
    size = "small",
)