licenses(["notice"])

package(default_visibility = ["//visibility:public"])

genrule(
    name = "cppcheck-checker",
    srcs = [],
    outs = ["cppcheck"],
    executable = 1,
    cmd = "cd external/com_github_danmar_cppcheck; make SRCDIR=build CFGDIR=cfg HAVE_RULES=yes CXXFLAGS='-O2 -DNDEBUG -Wall -Wno-sign-compare -Wno-unused-function'; cp -R $$(pwd)/* ../../$(@D)/",
)