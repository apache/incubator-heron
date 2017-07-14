package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")

load("/tools/rules/heron_api", "heron_api_files")

load("/tools/rules/heron_client", "heron_client_bin_files")
load("/tools/rules/heron_client", "heron_client_conf_files")
load("/tools/rules/heron_client", "heron_client_lib_third_party_files")
load("/tools/rules/heron_client", "heron_client_lib_scheduler_files")
load("/tools/rules/heron_client", "heron_client_lib_packing_files")
load("/tools/rules/heron_client", "heron_client_lib_statemgr_files")
load("/tools/rules/heron_client", "heron_client_lib_uploader_files")

load("/tools/rules/heron_core", "heron_core_files")
load("/tools/rules/heron_core", "heron_core_bin_files")
load("/tools/rules/heron_core", "heron_core_conf_files")
load("/tools/rules/heron_core", "heron_core_lib_instance_files")
load("/tools/rules/heron_core", "heron_core_lib_scheduler_files")
load("/tools/rules/heron_core", "heron_core_lib_metricsmgr_files")
load("/tools/rules/heron_core", "heron_core_lib_metricscachemgr_files")
load("/tools/rules/heron_core", "heron_core_lib_packing_files")
load("/tools/rules/heron_core", "heron_core_lib_statemgr_files")
load("/tools/rules/heron_core", "heron_core_lib_ckptmgr_files")
load("/tools/rules/heron_core", "heron_core_lib_statefulstorage_files")

load("/tools/rules/heron_tools", "heron_tools_files")
load("/tools/rules/heron_tools", "heron_tools_bin_files")
load("/tools/rules/heron_tools", "heron_tools_conf_files")
load("/tools/rules/heron_tools", "heron_tools_lib_files")
load("/tools/rules/heron_examples", "heron_examples_files")

generated_release_files = [
    "release.yaml",
]

generated_heronpy_egg_file = [
    "heronpy.whl",
]

filegroup(
    name = "release_files",
    srcs = generated_release_files,
)

filegroup(
    name = "tarpkgs",
    srcs = [
        ":heron-api",
        ":heron-core",
        ":heron-tools",
        ":heron-client",
    ],
)

filegroup(
    name = "binpkgs",
    srcs = [
        ":heron-api-install.sh",
        ":heron-client-install.sh",
        ":heron-tools-install.sh",
    ],
)

filegroup(
    name = "testpkgs",
    srcs = [
        ":heron-tests-install.sh",
    ],
)

filegroup(
    name = "pypkgs",
    srcs = [
        ":generate-heronpy-package",
    ],
)

################################################################################
# Heron core packaging
################################################################################
pkg_tar(
    name = "heron-core",
    extension = "tar.gz",
    files = generated_release_files,
    deps = [
        ":heron-core-bin",
        ":heron-core-lib-scheduler",
        ":heron-core-lib-packing",
        ":heron-core-lib-metricsmgr",
        ":heron-core-lib-metricscachemgr",
        ":heron-core-lib-statemgr",
        ":heron-core-lib-instance",
        ":heron-core-lib-ckptmgr",
        ":heron-core-lib-statefulstorage",
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
    name = "heron-core-lib-metricscachemgr",
    package_dir = "heron-core/lib/metricscachemgr",
    files = heron_core_lib_metricscachemgr_files(),
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
    name = "heron-core-lib-ckptmgr",
    package_dir = "heron-core/lib/ckptmgr",
    files = heron_core_lib_ckptmgr_files(),
)

pkg_tar(
    name = "heron-core-lib-statefulstorage",
    package_dir = "heron-core/lib/statefulstorage",
    files = heron_core_lib_statefulstorage_files(),
)

################################################################################
# Heron tools packaging
################################################################################
pkg_tar(
    name = "heron-tools-bin",
    package_dir = "bin",
    files = heron_tools_bin_files(),
)

pkg_tar(
    name = "heron-tools-conf",
    package_dir = "conf",
    files = heron_tools_conf_files(),
)

pkg_tar(
    name = "heron-tools",
    extension = "tar.gz",
    files = generated_release_files,
    deps = [
        ":heron-tools-bin",
        ":heron-tools-conf",
    ],
)

################################################################################
# Heron client packaging
################################################################################
pkg_tar(
    name = "heron-client-bin",
    package_dir = "bin",
    files = heron_client_bin_files(),
)

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
    name = "heron-client-lib-third_party",
    package_dir = "lib/third_party",
    files = heron_client_lib_third_party_files(),
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
    name = "heron-client-conf-slurm",
    package_dir = "conf/slurm",
    files = [
        "//heron/config/src/yaml:conf-slurm-yaml",
    ],
)

pkg_tar(
    name = "heron-client-conf-aurora",
    package_dir = "conf/aurora",
    files = [
        "//heron/config/src/yaml:conf-aurora-yaml",
    ],
)

pkg_tar(
    name = "heron-client-conf-yarn",
    package_dir = "conf/yarn",
    files = [
        "//heron/config/src/yaml:conf-yarn-yaml",
    ],
)

pkg_tar(
     name = "heron-client-conf-mesos",
     package_dir = "conf/mesos",
     files = [
         "//heron/config/src/yaml:conf-mesos-yaml",
     ],
)

pkg_tar(
    name = "heron-client-conf-marathon",
    package_dir = "conf/marathon",
    files = [
        "//heron/config/src/yaml:conf-marathon-yaml",
    ]
)

pkg_tar(
    name = "heron-client-conf-kubernetes",
    package_dir = "conf/kubernetes",
    files = [
        "//heron/config/src/yaml:conf-kubernetes-yaml",
    ]
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
    files = generated_release_files,
    deps = [
        ":heron-client-bin",
        ":heron-client-conf",
        ":heron-client-conf-local",
        ":heron-client-conf-aurora",
        ":heron-client-conf-slurm",
        ":heron-client-conf-yarn",
        ":heron-client-conf-mesos",
        ":heron-client-conf-marathon",
        ":heron-client-conf-kubernetes",
        ":heron-client-dist",
        ":heron-client-examples",
        ":heron-client-lib-third_party",
        ":heron-client-lib-scheduler",
        ":heron-client-lib-packing",
        ":heron-client-lib-statemgr",
        ":heron-client-lib-uploader",
    ],
)

################################################################################
# Heron test packaging
################################################################################
pkg_tar(
    name = "heron-tests-bin",
    package_dir = "bin",
    files = [
        "//integration_test/src/python/http_server:http-server",
        "//integration_test/src/python/test_runner:test-runner",
    ]
)

pkg_tar(
    name = "heron-tests-data-java",
    package_dir = "data/java",
    files = [
        "//integration_test/src/java:test-data-files",
    ],
)

pkg_tar(
    name = "heron-tests-data-python",
    package_dir = "data/python",
    files = [
        "//integration_test/src/python/integration_test/topology:test-data-files",
    ],
)

pkg_tar(
    name = "heron-tests-lib",
    package_dir = "lib",
    files = [
       "//integration_test/src/java:integration-tests",
       "//integration_test/src/python/integration_test/topology:heron_integ_topology",
    ],
)

pkg_tar(
    name = "heron-tests",
    extension = "tar.gz",
    files = generated_release_files,
    deps = [
        ":heron-tests-bin",
        ":heron-tests-data-java",
        ":heron-tests-data-python",
        ":heron-tests-lib",
    ],
)

################################################################################
# Heron API packaging
################################################################################
pkg_tar(
    name = "heron-api",
    extension = "tar.gz",
    files = generated_release_files + heron_api_files(),
)

################################################################################
# Heron python packaging
# 
# When a new python module is being added into the heronpy package, please ensure 
# that its dependencies are being added into the packaging as well. Sometimes,
# the dependencies might already exist. For example, 
#   heron/api depends on proto
#   heron/dsl depends on proto and heron/api
#
# Hence the python package consists of the dependencies proto and heron/api.
# Note that the heron/api is exported as external API for heronpy and it is
# duplicated for dependency for heron/dsl.
################################################################################
genrule(
    name = "generate-heronpy-package",
    srcs = [
        "heronpy/requirements.txt", 
        "heronpy/setup.py.template",
        "heronpy/README.txt",
    ] + generated_release_files,
    outs = generated_heronpy_egg_file,
    stamp = 1,
    tools = [
        "//heron/api/src/python:heron-python-package",
        "//heron/dsl/src/python:heron-pythondsl-package",
    ],
    cmd = "\n".join([
        'export OUTPUT_DIR=$$(pwd)/$(@D)',
        'export RELEASE_FILE_DIR=$$(pwd)',
        'export TMP_DIR=$$(mktemp -d -t heronpy.XXXXX)',
        'echo $$TMP_DIR',
        'export HERONPY_DIR=$$TMP_DIR/heronpy',
        'export HERONPYAPI_UNZIP=$$TMP_DIR/heronapiunzip',
        'export HERONPYDSL_UNZIP=$$TMP_DIR/herondslunzip',
        'export HERON_VERSION=$$(grep version $$RELEASE_FILE_DIR/$(location :release.yaml) | awk \'{print $$3}\')',
        'export HERON_VERSION=$$(echo $$HERON_VERSION | sed -e "s/^\'//" -e "s/\'$$//")',
        'export HERON_VERSION=$$(echo $$HERON_VERSION | grep "[0-9]*\.[0-9]*\.[0-9]*")',
        'export HERON_VERSION=$$([[ -z $$HERON_VERSION ]] && echo "0.0.0" || echo $$HERON_VERSION)',
        'echo $$HERON_VERSION',
        'mkdir -p $$TMP_DIR $$HERONPY_DIR',
        'unzip -qd $$HERONPYAPI_UNZIP $(location //heron/api/src/python:heron-python-package)',
        'unzip -qd $$HERONPYDSL_UNZIP $(location //heron/dsl/src/python:heron-pythondsl-package)',
        'find $$HERONPYAPI_UNZIP -name "*.pyc" -exec rm {} \;',
        'find $$HERONPYDSL_UNZIP -name "*.pyc" -exec rm {} \;',
        'mv $$HERONPYAPI_UNZIP/heron/api/src/python $$HERONPY_DIR/heronpy',
        'mv $$HERONPYDSL_UNZIP/heron/dsl/src/python $$HERONPY_DIR/heronpy/streamlet',
        'rm -rf $$HERONPYAPI_UNZIP/heron/api',
        'rm -rf $$HERONPYDSL_UNZIP/heron/dsl',
        'mv $$HERONPYDSL_UNZIP/heron $$HERONPY_DIR/heron',
        'cp $(SRCS) $$HERONPY_DIR',
        'echo "Heron Python toplevel directory: $$HERONPY_DIR"',
        'cd $$HERONPY_DIR',
        'sed "s/VERSION/$$HERON_VERSION/" setup.py.template > setup.py',
        'rm setup.py.template',
        '/usr/bin/env python2.7 setup.py sdist',
        '/usr/bin/env python2.7 setup.py bdist_wheel',
        'ls -l $$HERONPY_DIR/dist',
        'cp $$HERONPY_DIR/dist/heronpy-*-py2-*.whl $$OUTPUT_DIR',
        'cp $$HERONPY_DIR/dist/heronpy-*.tar.gz $$OUTPUT_DIR',
        'touch $$OUTPUT_DIR/heronpy.whl',
        'rm -rf $$TMP_DIR',
    ]),
)

################################################################################
# Heron self installed binaries
################################################################################
sh_binary(
    name = "package-info-generator",
    srcs = ["package_info_generator.sh"],
)

genrule(
    name = "generate-package-info",
    outs = generated_release_files,
    cmd = "$(location :package-info-generator) $$(find -L . -name '*status*.txt') >$@",
    stamp = 1,
    tools = [":package-info-generator"],
)

genrule(
    name = "generate-client-launcher",
    srcs = ["client_template_bin.sh", "bin_common.sh"] + generated_release_files,
    outs = ["client_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location bin_common.sh))"
        template="$$(cat $(location client_template_bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

genrule(
    name = "generate-tools-launcher",
    srcs = ["tools_template_bin.sh", "bin_common.sh"] + generated_release_files,
    outs = ["tools_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location bin_common.sh))"
        template="$$(cat $(location tools_template_bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

genrule(
    name = "generate-api-launcher",
    srcs = ["api_template_bin.sh", "bin_common.sh"] + generated_release_files,
    outs = ["api_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location bin_common.sh))"
        template="$$(cat $(location api_template_bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

genrule(
    name = "generate-tests-launcher",
    srcs = ["tests_template_bin.sh", "bin_common.sh"] + generated_release_files,
    outs = ["tests_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location bin_common.sh))"
        template="$$(cat $(location tests_template_bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

load("self_extract_binary", "self_extract_binary")

self_extract_binary(
    name = "heron-api-install.sh",
    flatten_resources = [
        ":heron-api",
    ],
    launcher = ":api_launcher_bin.sh",
)

self_extract_binary(
    name = "heron-client-install.sh",
    flatten_resources = [
        ":heron-client",
    ],
    launcher = ":client_launcher_bin.sh",
)

self_extract_binary(
    name = "heron-tools-install.sh",
    flatten_resources = [
        ":heron-tools",
    ],
    launcher = ":tools_launcher_bin.sh",
)

self_extract_binary(
    name = "heron-tests-install.sh",
    flatten_resources = [
        ":heron-tests",
    ],
    launcher = ":tests_launcher_bin.sh",
)

filegroup(
    name = "api-template-bin.sh",
    srcs = ["api_template_bin.sh"],
)

filegroup(
    name = "client-template-bin.sh",
    srcs = ["client_template_bin.sh"],
)

filegroup(
    name = "tools-template-bin.sh",
    srcs = ["tools_template_bin.sh"],
)

filegroup(
    name = "tests-template-bin.sh",
    srcs = ["tests_template_bin.sh"],
)

filegroup(
    name = "bin-common.sh",
    srcs = ["bin_common.sh"],
)
