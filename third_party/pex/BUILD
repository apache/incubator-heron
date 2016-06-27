licenses(["notice"])

package(default_visibility = ["//visibility:public"])

py_library(
    name = 'pex',
    srcs = glob(['**/*.py']),
    deps = [
        "//third_party/setuptools:pkg_resources",
    ]
)

py_binary(
    name = '_pex',
    srcs = ["_pex.py"],
    deps = [ 
        ":pex",
    ],
)
