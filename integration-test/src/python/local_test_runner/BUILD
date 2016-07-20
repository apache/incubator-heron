package(default_visibility = ["//visibility:public"])

pex_binary(
    name = "local-test-runner",
    srcs = [
        "main.py"
    ],
    data = [
        "resources/test.conf",
    ],
    reqs = ["argparse==1.4.0"],
)
