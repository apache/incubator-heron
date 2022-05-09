package(default_visibility = ["//visibility:public"])

pex_library(
    name = "heron-ui-lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["main.py"],
    ),
    reqs = [
        "requests==2.27.1",
        "click==7.1.2",
        "fastapi==0.75.0",
        "jinja2==3.0.3",
        "aiofiles==0.5.0",
        "uvicorn==0.11.7",
        "uvloop==0.16.0",
    ],
    deps = [
        "//heron/common/src/python:common-py",
        "//heron/tools/common/src/python:common-py",
        "//heron/tools/common/src/python:tracker-py",
    ],
)

pex_binary(
    name = "heron-ui",
    srcs = ["main.py"],
    resources = [
        "//heron/tools/ui/resources:static",
        "//heron/tools/ui/resources:templates",
        "//scripts/packages:release_files",
    ],
    zip_safe = False,
    deps = [
        ":heron-ui-lib",
    ],
)
