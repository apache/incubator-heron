licenses(["notice"])

package(default_visibility = ["//visibility:public"])

genrule(
    name = "nomad-scheduler",
    srcs = ["nomad"],
    outs = ["heron-nomad"],
    cmd = "mv $< $@",
)
