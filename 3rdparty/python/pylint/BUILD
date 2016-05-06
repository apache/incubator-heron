package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_binary(
    name = "pylint",
    main = "main.py",
    srcs = ["main.py"],
    eggs = [
        "//3rdparty/eggs:astroid",
        "//3rdparty/eggs:colorama",
        "//3rdparty/eggs:lazy-object-proxy",
        "//3rdparty/eggs:pylint",
        "//3rdparty/eggs:six",
        "//3rdparty/eggs:wrapt",
    ],
)
