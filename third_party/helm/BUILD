licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "helm",
    srcs = select({
        "@platforms//os:osx": ["@helm_mac//:helm-exec"],
        "//conditions:default": ["@helm_linux//:helm-exec"],
    }),
)
