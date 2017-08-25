package(default_visibility = ["//visibility:public"])

genrule(
  name = "heron-downloader-shell",
  srcs = [":heron-downloader.sh"],
  outs = ["heron-downloader"],
  cmd  = "cp $< $@",
)
