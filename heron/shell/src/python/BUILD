package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-shell-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"]
    ),
    reqs = [
        "requests==2.11.1",
        "tornado==4.0.2",
    ],
)

pex_binary(
    name = "heron-shell",
    srcs = ["main.py"],
    deps = [
        ":heron-shell-lib",
    ],
    resources = [
        "//heron/shell/assets:assets",
    ],
)
