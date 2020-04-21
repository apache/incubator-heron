# Scripts for building Heron releases
package(default_visibility = ["//visibility:private"])

sh_library(
    name = "relnotes",
    srcs = ["relnotes.sh"],
)

sh_library(
    name = "release",
    srcs = [
        "common.sh",
        "release.sh",
    ],
    deps = [":relnotes"],
)

filegroup(
    name = "workspace-status",
    srcs = [
        "status.sh",
    ],
)
