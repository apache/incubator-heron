licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "thrift",
    srcs = select({
        "//tools/platform:darwin": ["thrift-osx-x86_64.exe"],
        "//tools/platform:k8": ["thrift-linux-x86_64.exe"],
    }),
)
