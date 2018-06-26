package(default_visibility = ["//visibility:public"])

dlog_deps = [
  "@org_apache_distributedlog_core//jar",
  "//third_party/java:dlog-java",
]

java_library(
  name = 'dlog-lib',
  srcs = glob(["**/dlog/DL*.java"]),
  deps = dlog_deps,
)

java_binary(
  name = "dlog-util-unshaded",
  srcs = glob(["**/dlog/*.java"]),
  main_class = "org.apache.heron.dlog.Util",
  deps = dlog_deps + ["//heron/common/src/java:basics-java"]
)

genrule(
  name = "dlog-util",
  srcs = [":dlog-util-unshaded_deploy.jar"],
  outs = ["dlog-util.jar"],
  cmd  = "cp $< $@",
)
