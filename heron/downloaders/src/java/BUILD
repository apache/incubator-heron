package(default_visibility = ["//visibility:public"])

downloader_deps = [
  "@org_apache_distributedlog_core//jar",
  "//heron/spi/src/java:common-spi-java",
  "//heron/io/dlog/src/java:dlog-lib",
  "//third_party/java:commons-compress",
]

java_binary(
  name = 'heron-downloader-unshaded',
  srcs = glob(["**/downloader/*.java"]),
  main_class = "org.apache.heron.downloader.DownloadRunner",
  deps = downloader_deps,
)

genrule(
  name = "heron-downloader",
  srcs = [":heron-downloader-unshaded_deploy.jar"],
  outs = ["heron-downloader.jar"],
  cmd  = "cp $< $@",
)
