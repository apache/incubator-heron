licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "dev-job",
    srcs = ["heron-ui-devel.aurora"],
)

filegroup(
    name = "prod-job",
    srcs = ["heron-ui.aurora"],
)

sh_binary(
    name = "dev",
    srcs = ["deploy-dev.sh"],
    data = [
        "//heron/ui/src/python:heron-ui",
        "//heron/ui/deploy/aurora:dev-job",
    ],
    args = [
        "$(location //heron/ui/src/python:heron-ui)",
        "$(location //heron/ui/deploy/aurora:dev-job)",
    ],
)

sh_binary(
    name = "prod",
    srcs = ["deploy-prod.sh"],
    data = [
        "//heron/ui/src/python:heron-ui",
        "//heron/ui/deploy/aurora:prod-job",
    ],
    args = [
        "$(location //heron/ui/src/python:heron-ui)",
        "$(location //heron/ui/deploy/aurora:prod-job)",
    ],
)
