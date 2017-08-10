licenses(["notice"]) # apache 2.0

package(default_visibility = ["//visibility:public"])

load(
    "@io_bazel_rules_pex//pex:pex_rules.bzl",
    "pex_binary",
    "pex_library",
    "pex_test",
    "pex_pytest",
)

pex_binary(
    name = "pylint",
    main = "main.py",
    srcs = ["main.py"],
    reqs = ["pylint==1.5.5"],
)
