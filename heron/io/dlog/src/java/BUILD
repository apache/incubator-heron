package(default_visibility = ["//visibility:public"])

dlog_deps = [
  "@com_google_guava_guava//jar",
  "@org_apache_distributedlog_core//jar",
  "//third_party/java:dlog-java",
]

java_library(
  name = 'dlog-lib',
  srcs = glob(["**/dlog/*.java"]),
  deps = dlog_deps,
)
