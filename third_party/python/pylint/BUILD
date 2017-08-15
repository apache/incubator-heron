licenses(["notice"]) # apache 2.0

package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
    name = "pylint",
    main = "main.py",
    srcs = ["main.py"],
    reqs = [
            "pylint==1.7.2",
            "astroid>=1.5.1",
            "singledispatch==3.4.0.3",
            "enum34>=1.1.3"],
)
