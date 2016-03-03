package(default_visibility = ["//visibility:public"])

load("/tools/build_defs/pkg/pkg", "pkg_tar")

load("/tools/rules/heron", "heron_api_files")
load("/tools/rules/heron", "heron_api_bin_files")
load("/tools/rules/heron", "heron_api_conf_files")
load("/tools/rules/heron", "heron_api_lib_files")

load("/tools/rules/heron", "heron_cli_files")
load("/tools/rules/heron", "heron_cli_bin_files")
load("/tools/rules/heron", "heron_cli_conf_files")
load("/tools/rules/heron", "heron_cli_lib_files")

load("/tools/rules/heron", "heron_core_files")
load("/tools/rules/heron", "heron_core_bin_files")
load("/tools/rules/heron", "heron_core_conf_files")
load("/tools/rules/heron", "heron_core_lib_files")

load("/tools/rules/heron", "heron_metrics_api_files")
load("/tools/rules/heron", "heron_metrics_api_bin_files")
load("/tools/rules/heron", "heron_metrics_api_conf_files")
load("/tools/rules/heron", "heron_metrics_api_lib_files")

load("/tools/rules/heron", "heron_storm_compat_files")
load("/tools/rules/heron", "heron_storm_compat_bin_files")
load("/tools/rules/heron", "heron_storm_compat_conf_files")
load("/tools/rules/heron", "heron_storm_compat_lib_files")

load("/tools/rules/heron", "heron_tracker_files")
load("/tools/rules/heron", "heron_tracker_bin_files")
load("/tools/rules/heron", "heron_tracker_conf_files")
load("/tools/rules/heron", "heron_tracker_lib_files")

load("/tools/rules/heron", "heron_bin_files")
load("/tools/rules/heron", "heron_conf_files")
load("/tools/rules/heron", "heron_lib_files")

filegroup(
    name = "packages",
    srcs = [
        ":heron-api",
        ":heron-cli",
        ":heron-core",
        ":heron-storm-compat",
        ":heron-tracker",
        ":heron",
    ],
)

pkg_tar(
    name = "heron-api",
    extension = "tar.gz",
    files = heron_api_files(),
)

pkg_tar(
    name = "heron-cli-bin",
    package_dir = "bin",
    files = heron_cli_bin_files(),
)

pkg_tar(
    name = "heron-cli-lib",
    package_dir = "lib",
    files = heron_cli_lib_files(),
)

pkg_tar(
    name = "heron-cli-top",
    files = heron_cli_conf_files(),
)
    
pkg_tar(
    name = "heron-cli",
    extension = "tar.gz",
    deps = [
        ":heron-cli-top",
        ":heron-cli-bin",
        ":heron-cli-lib",
    ],
)

pkg_tar(
    name = "heron-core",
    extension = "tar.gz",
    files = heron_core_files(), 
)

pkg_tar(
    name = "heron-storm-compat",
    extension = "tar.gz",
    files = heron_storm_compat_files(),
)

pkg_tar(
    name = "heron-tracker",
    extension = "tar.gz",
    files = heron_tracker_files(),
)

pkg_tar(
    name = "heron-bin",
    package_dir = "bin",
    extension = "tar.gz",
    files = heron_bin_files(),
    mode = "0755",
)

pkg_tar(
    name = "heron-conf",
    package_dir = "conf",
    extension = "tar.gz",
    files = heron_conf_files(),
    mode = "0644",
)

pkg_tar(
    name = "heron-lib",
    package_dir = "lib",
    extension = "tar.gz",
    files = heron_lib_files(),
    mode = "0644",
)

pkg_tar(
    name = "heron",
    extension = "tar.gz",
    deps = [
        ":heron-bin",
        ":heron-conf",
        ":heron-lib",
    ],
)
