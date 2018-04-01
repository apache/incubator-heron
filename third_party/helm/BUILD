licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "helm",
    srcs = select({
        "//tools/platform:darwin": ["@helm_mac//:helm-exec"],
        "//conditions:default": ["@helm_linux//:helm-exec"],
    }),
)
