licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "heron-nomad",
    srcs = select({
        "//tools/platform:darwin": ["@nomad_mac//:nomad-bin"],
        "//conditions:default": ["@nomad_linux//:nomad-bin"],
    }),
)
