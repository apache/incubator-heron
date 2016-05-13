package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_binary")

pex_binary(
    name = "local-test-runner",
    srcs = [
        "main.py"
    ],
    main = "main.py",
    resources = [
        "resources/test.conf",
    ],
    reqs = ["argparse==1.4.0"],
)
