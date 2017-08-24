load("/tools/rules/java_tests", "java_tests")

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
  "//third_party/java:mockito",
  "//third_party/java:junit4",
]

dlog_test_deps_files = \
  common_deps_files + [
    "@org_apache_distributedlog_core//jar",
    "@org_apache_distributedlog_protocol//jar",
    "//heron/io/dlog/src/java:dlog-lib",
  ]
  
java_test(
    name = "DLInputStreamTest",
    srcs = glob(["**/dlog/DLInputStreamTest.java"]),
    deps = dlog_test_deps_files,
    size = "small",
)

java_test(
    name = "DLOutputStreamTest",
    srcs = glob(["**/dlog/DLOutputStreamTest.java"]),
    deps = dlog_test_deps_files,
    size = "small",
)
