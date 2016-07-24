licenses(["notice"]) # apache 2.0

package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
    name = "pylint",
    main = "main.py",
    srcs = ["main.py"],
    reqs = ["pylint==1.5.5"],
)
