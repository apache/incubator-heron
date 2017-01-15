licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "thrift",
    srcs = select({
        ":darwin": ["thrift-osx-x86_64.exe"],
        ":k8": ["thrift-linux-x86_64.exe"],
    }),
)

config_setting(
    name = "darwin",
    values = {"host_cpu": "darwin"},
)

config_setting(
    name = "k8",
    values = {"host_cpu": "k8"},
)
