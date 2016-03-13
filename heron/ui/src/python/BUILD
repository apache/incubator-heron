package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-ui-lib",
    srcs = glob([
        "handlers/**/*.py",
    ]),
    eggs = [
        "//3rdparty/eggs:tornado",
        "//3rdparty/eggs:pycrypto",
        "//3rdparty/eggs:requests",
    ]
)

pex_binary(
    name = "heron-ui",
    srcs = ["main.py"],
    deps = [
        ":heron-ui-lib",
    ],
    resources = [
        "//heron/ui/resources:templates",
        "//heron/ui/resources:static",
    ],
    zip_safe = False,
)
