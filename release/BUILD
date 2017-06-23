package(default_visibility = ["//visibility:public"])

load("/tools/build_defs/pkg/pkg", "pkg_tar")

# load("/tools/rules/heron_client", "heron_client_files")
load("/tools/rules/heron_client", "heron_client_bin_files")
load("/tools/rules/heron_client", "heron_client_conf_files")
# load("/tools/rules/heron_client", "heron_client_lib_files")
load("/tools/rules/heron_client", "heron_client_lib_scheduler_files")
load("/tools/rules/heron_client", "heron_client_lib_packing_files")
load("/tools/rules/heron_client", "heron_client_lib_statemgr_files")
load("/tools/rules/heron_client", "heron_client_lib_uploader_files")
load("/tools/rules/heron_client", "heron_client_lib_healthmgr_files")

load("/tools/rules/heron_core", "heron_core_files")
load("/tools/rules/heron_core", "heron_core_bin_files")
load("/tools/rules/heron_core", "heron_core_conf_files")
load("/tools/rules/heron_core", "heron_core_lib_instance_files")
load("/tools/rules/heron_core", "heron_core_lib_scheduler_files")
load("/tools/rules/heron_core", "heron_core_lib_metricsmgr_files")
load("/tools/rules/heron_core", "heron_core_lib_packing_files")
load("/tools/rules/heron_core", "heron_core_lib_statemgr_files")

load("/tools/rules/heron_tools", "heron_tools_files")
load("/tools/rules/heron_tools", "heron_tools_bin_files")
load("/tools/rules/heron_tools", "heron_tools_conf_files")
load("/tools/rules/heron_tools", "heron_tools_lib_files")

load("/tools/rules/heron_examples", "heron_examples_files")

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
        ":heron-core-lib-scheduler",
        ":heron-core-lib-packing",
        ":heron-core-lib-metricsmgr",
        ":heron-core-lib-statemgr",
        ":heron-core-lib-instance",
    ],
)

pkg_tar(
    name = "heron-core-bin",
    package_dir = "heron-core/bin",
    files = heron_core_bin_files(), 
)

pkg_tar(
    name = "heron-core-lib-scheduler",
    package_dir = "heron-core/lib/scheduler",
    files = heron_core_lib_scheduler_files(), 
)

pkg_tar(
    name = "heron-core-lib-packing",
    package_dir = "heron-core/lib/packing",
    files = heron_core_lib_packing_files(), 
)

pkg_tar(
    name = "heron-core-lib-metricsmgr",
    package_dir = "heron-core/lib/metricsmgr",
    files = heron_core_lib_metricsmgr_files(), 
)

pkg_tar(
    name = "heron-core-lib-statemgr",
    package_dir = "heron-core/lib/statemgr",
    files = heron_core_lib_statemgr_files(), 
)

pkg_tar(
    name = "heron-core-lib-instance",
    package_dir = "heron-core/lib/instance",
    files = heron_core_lib_instance_files(), 
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

# pkg_tar(
#     name = "heron-client-lib",
#     package_dir = "lib",
#     files = heron_client_lib_files(),
# )

pkg_tar(
    name = "heron-client-lib-scheduler",
    package_dir = "lib/scheduler",
    files = heron_client_lib_scheduler_files(),
)

pkg_tar(
    name = "heron-client-lib-packing",
    package_dir = "lib/packing",
    files = heron_client_lib_packing_files(),
)

pkg_tar(
    name = "heron-client-lib-statemgr",
    package_dir = "lib/statemgr",
    files = heron_client_lib_statemgr_files(),
)

pkg_tar(
    name = "heron-client-lib-uploader",
    package_dir = "lib/uploader",
    files = heron_client_lib_uploader_files(),
)

pkg_tar(
    name = "heron-client-lib-healthmgr",
    package_dir = "lib/healthmgr",
    files = heron_client_lib_healthmgr_files(),
)

pkg_tar(
    name = "heron-client-examples",
    package_dir = "examples",
    files = heron_examples_files(),
)

pkg_tar(
    name = "heron-client-conf-local",
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
        ":heron-client-conf-local",
        ":heron-client-dist",
        ":heron-client-examples",
        ":heron-client-lib-scheduler",
        ":heron-client-lib-packing",
        ":heron-client-lib-statemgr",
        ":heron-client-lib-uploader",
        ":heron-client-lib-healthmgr",
    ],
)
