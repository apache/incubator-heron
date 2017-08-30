package(default_visibility = ["//visibility:public"])

downloader_deps = [
  "//third_party/java:commons-compress",
]

java_binary(
  name = 'heron-downloader-unshaded',
  srcs = glob(["**/downloader/*.java"]),
  main_class = "com.twitter.heron.downloader.DownloadRunner",
  deps = downloader_deps,
)

genrule(
  name = "heron-downloader",
  srcs = [":heron-downloader-unshaded_deploy.jar"],
  outs = ["heron-downloader.jar"],
  cmd  = "cp $< $@",
)
