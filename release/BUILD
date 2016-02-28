package(default_visibility = ["//visibility:public"])

load("/tools/build_defs/pkg/pkg", "pkg_tar")

load("/tools/rules/heron_client", "heron_client_files")
load("/tools/rules/heron_client", "heron_client_bin_files")
load("/tools/rules/heron_client", "heron_client_conf_files")
load("/tools/rules/heron_client", "heron_client_lib_files")

load("/tools/rules/heron_core", "heron_core_files")
load("/tools/rules/heron_core", "heron_core_bin_files")
load("/tools/rules/heron_core", "heron_core_conf_files")
load("/tools/rules/heron_core", "heron_core_lib_files")

load("/tools/rules/heron_tools", "heron_tools_files")
load("/tools/rules/heron_tools", "heron_tools_bin_files")
load("/tools/rules/heron_tools", "heron_tools_conf_files")
load("/tools/rules/heron_tools", "heron_tools_lib_files")

filegroup(
    name = "packages",
    srcs = [
        ":heron-core",
        ":heron-tools",
        ":heron-client",
    ],
)

pkg_tar(
    name = "heron-core",
    extension = "tar.gz",
    deps = [
        ":heron-core-bin",
        ":heron-core-lib",
    ],
)

pkg_tar(
    name = "heron-core-bin",
    package_dir = "heron-core/bin",
    files = heron_core_bin_files(), 
)

pkg_tar(
    name = "heron-core-lib",
    package_dir = "heron-core/lib",
    files = heron_core_lib_files(), 
)

pkg_tar(
    name = "heron-tools",
    extension = "tar.gz",
    files = heron_tools_files(),
)

pkg_tar(
    name = "heron-client-bin",
    package_dir = "bin",
    files = heron_client_bin_files(),
)

pkg_tar(
    name = "heron-client-lib",
    package_dir = "lib",
    files = heron_client_lib_files(),
)

pkg_tar(
    name = "heron-client-local",
    package_dir = "conf/local",
    files = [
        "//heron/config/src/yaml:conf-local-yaml",
    ],
)
 
pkg_tar(
    name = "heron-client-conf",
    package_dir = "conf",
    files = heron_client_conf_files(),
)

pkg_tar(
    name = "heron-client-dist",
    package_dir = "dist",
    files = [":heron-core"],
)
    
pkg_tar(
    name = "heron-client",
    extension = "tar.gz",
    deps = [
        ":heron-client-bin",
        ":heron-client-conf",
        ":heron-client-local",
        ":heron-client-dist",
        ":heron-client-lib",
    ],
)
