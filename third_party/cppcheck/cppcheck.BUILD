licenses(["notice"])

package(default_visibility = ["//visibility:public"])

install_script = "\n".join([
    "cd external/com_github_danmar_cppcheck",
    "make SRCDIR=build CFGDIR=cfg CXXFLAGS='-O2 -DNDEBUG -Wall -Wno-sign-compare -Wno-unused-function'",
    "rm -rf ../../$(@D)/*",
    "cp -R $$(pwd)/* ../../$(@D)/",
])

genrule(
    name = "cppcheck-checker",
    srcs = [],
    outs = ["cppcheck"],
    executable = 1,
    cmd = install_script,
)