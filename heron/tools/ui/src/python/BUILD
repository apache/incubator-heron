package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:defs.bzl", "py_binary")
load("@heron_py_deps//:requirements.bzl", "requirement")

py_library(
    name = "heron-ui-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
        requirement("requests"),
        requirement("click"),
        requirement("fastapi"),
        requirement("jinja2"),
        requirement("aiofiles"),
        requirement("uvicorn"),
        requirement("uvloop"),
    ],
)

py_binary(
    name = "heron-ui",
    main = "main.py",
    srcs = ["main.py"],
    resources = [
        "//heron/tools/ui/resources:static",
        "//heron/tools/ui/resources:templates",
        "//scripts/packages:release_files",
    ],
    # zip_safe = False, ## pex_binary attribute not available in py_binary
    deps = [
        ":heron-ui-lib",
    ],
)
