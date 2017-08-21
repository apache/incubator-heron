package(default_visibility = ["//visibility:public"])

genrule(
  name = "heron-downloader",
  srcs = [":heron-downloader.sh"],
  outs = ["heron-downloader"],
  cmd  = "cp $< $@",
)
