load("@rules_cc//cc:defs.bzl", "cc_toolchain")
load(":cc_toolchain_config.bzl", "cc_toolchain_config")

licenses(["restricted"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "empty",
    srcs = [],
)

# This is the entry point for --crosstool_top.  Toolchains are found
# by lopping off the name of --crosstool_top and searching for
# "cc-compiler-${CPU}" in this BUILD file, where CPU is the target CPU
# specified in --cpu.
#
# This file group should include
#   * all cc_toolchain targets supported
#   * all file groups that said cc_toolchain might refer to,
# including the default_grte_top setting in the CROSSTOOL
# protobuf.
filegroup(
    name = "toolchain",
    srcs = [
        ":cc-compiler-local",
        ":empty",
    ],
)

cc_toolchain_config(name = "k8_toolchain_config")

cc_toolchain(
    name = "cc-compiler-local",
    all_files = ":empty",
    compiler_files = ":empty",
    dwp_files = ":empty",
    linker_files = ":empty",
    objcopy_files = ":empty",
    strip_files = ":empty",
    supports_param_files = 0,
    toolchain_config = ":k8_toolchain_config",
)

cc_toolchain(
    name = "cc-compiler-k8",
    all_files = ":empty",
    compiler_files = ":empty",
    dwp_files = ":empty",
    linker_files = ":empty",
    objcopy_files = ":empty",
    strip_files = ":empty",
    supports_param_files = 0,
    toolchain_config = ":k8_toolchain_config",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
