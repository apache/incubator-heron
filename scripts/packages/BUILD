package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")

load("//tools/rules:heron_client.bzl", "heron_client_bin_files")
load("//tools/rules:heron_client.bzl", "heron_client_conf_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_third_party_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_scheduler_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_metricscachemgr_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_packing_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_statemgr_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_uploader_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_downloader_files")
load("//tools/rules:heron_client.bzl", "heron_client_lib_healthmgr_files")

load("//tools/rules:heron_core.bzl", "heron_core_files")
load("//tools/rules:heron_core.bzl", "heron_core_bin_files")
load("//tools/rules:heron_core.bzl", "heron_core_conf_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_instance_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_healthmgr_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_metricsmgr_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_metricscachemgr_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_packing_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_scheduler_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_statemgr_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_ckptmgr_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_statefulstorage_files")
load("//tools/rules:heron_core.bzl", "heron_core_lib_downloader_files")

load("//tools/rules:heron_tools.bzl", "heron_tools_files")
load("//tools/rules:heron_tools.bzl", "heron_tools_bin_files")
load("//tools/rules:heron_tools.bzl", "heron_tools_bin_all_files")
load("//tools/rules:heron_tools.bzl", "heron_tools_conf_files")
load("//tools/rules:heron_tools.bzl", "heron_tools_lib_files")
load("//tools/rules:heron_examples.bzl", "heron_examples_files")

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
        ":heron-tools",
        ":heron-core",
        ":heron",
    ],
)

filegroup(
    name = "binpkgs",
    srcs = [
        ":heron-install.sh",
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
# Heron core packaging - used for docker images
################################################################################
pkg_tar(
    name = "heron-core",
    extension = "tar.gz",
    srcs = generated_release_files,
    deps = [
        ":heron-core-bin",
        ":heron-core-lib-scheduler",
        ":heron-core-lib-packing",
        ":heron-core-lib-healthmgr",
        ":heron-core-lib-metricsmgr",
        ":heron-core-lib-metricscachemgr",
        ":heron-core-lib-statemgr",
        ":heron-core-lib-instance",
        ":heron-core-lib-ckptmgr",
        ":heron-core-lib-statefulstorage",
        ":heron-core-lib-downloader",
    ],
)

pkg_tar(
    name = "heron-core-bin",
    package_dir = "heron-core/bin",
    srcs = heron_core_bin_files(),
)

pkg_tar(
    name = "heron-core-lib-scheduler",
    package_dir = "heron-core/lib/scheduler",
    srcs = heron_core_lib_scheduler_files(),
)

pkg_tar(
    name = "heron-core-lib-packing",
    package_dir = "heron-core/lib/packing",
    srcs = heron_core_lib_packing_files(),
)

pkg_tar(
    name = "heron-core-lib-healthmgr",
    package_dir = "heron-core/lib/healthmgr",
    srcs = heron_core_lib_healthmgr_files(),
)

pkg_tar(
    name = "heron-core-lib-metricsmgr",
    package_dir = "heron-core/lib/metricsmgr",
    srcs = heron_core_lib_metricsmgr_files(),
)

pkg_tar(
    name = "heron-core-lib-metricscachemgr",
    package_dir = "heron-core/lib/metricscachemgr",
    srcs = heron_core_lib_metricscachemgr_files(),
)

pkg_tar(
    name = "heron-core-lib-statemgr",
    package_dir = "heron-core/lib/statemgr",
    srcs = heron_core_lib_statemgr_files(),
)

pkg_tar(
    name = "heron-core-lib-instance",
    package_dir = "heron-core/lib/instance",
    srcs = heron_core_lib_instance_files(),
)

pkg_tar(
    name = "heron-core-lib-ckptmgr",
    package_dir = "heron-core/lib/ckptmgr",
    srcs = heron_core_lib_ckptmgr_files(),
)

pkg_tar(
    name = "heron-core-lib-statefulstorage",
    package_dir = "heron-core/lib/statefulstorage",
    srcs = heron_core_lib_statefulstorage_files(),
)

pkg_tar(
    name = "heron-core-lib-downloader",
    package_dir = "heron-core/lib/downloaders",
    srcs = heron_core_lib_downloader_files(),
)

################################################################################
# Heron tools packaging - used for docker images
################################################################################
pkg_tar(
    name = "heron-tools-release",
    package_dir = "heron-tools",
    srcs = generated_release_files,
)

pkg_tar(
    name = "heron-tools-bin",
    package_dir = "heron-tools/bin",
    srcs = heron_tools_bin_all_files(),
)

pkg_tar(
    name = "heron-tools-lib-api",
    package_dir = "heron-tools/lib/api",
    srcs = heron_tools_lib_files(),
)

pkg_tar(
    name = "heron-tools-lib-third_party",
    package_dir = "heron-tools/lib/third_party",
    srcs = heron_client_lib_third_party_files(),
)

pkg_tar(
    name = "heron-tools-lib-uploader",
    package_dir = "heron-tools/lib/uploader",
    srcs = heron_client_lib_uploader_files(),
)

pkg_tar(
    name = "heron-tools-conf",
    package_dir = "heron-tools/conf",
    srcs = heron_tools_conf_files()
)

pkg_tar(
    name = "heron-tools-cluster-conf",
    strip_prefix = "/heron/config/src/yaml/conf",
    package_dir = "heron-tools/conf",
    srcs = [
      "//heron/config/src/yaml:conf-yaml"
    ],
)

pkg_tar(
    name = "heron-tools",
    extension = "tar.gz",
    deps = [
        ":heron-tools-release",
        ":heron-tools-bin",
        ":heron-tools-lib-api",
        ":heron-tools-lib-third_party",
        ":heron-tools-lib-uploader",
        ":heron-tools-conf",
        ":heron-tools-cluster-conf",
    ],
)

################################################################################
# Heron packaging
################################################################################
pkg_tar(
    name = "heron-bin",
    package_dir = "bin",
    srcs = heron_client_bin_files() + heron_tools_bin_files(),
)

pkg_tar(
    name = "heron-lib-scheduler",
    package_dir = "lib/scheduler",
    srcs = heron_client_lib_scheduler_files(),
)

pkg_tar(
    name = "heron-lib-metricscachemgr",
    package_dir = "lib/metricscachemgr",
    srcs = heron_client_lib_metricscachemgr_files(),
)

pkg_tar(
    name = "heron-lib-packing",
    package_dir = "lib/packing",
    srcs = heron_client_lib_packing_files(),
)

pkg_tar(
    name = "heron-lib-statemgr",
    package_dir = "lib/statemgr",
    srcs = heron_client_lib_statemgr_files(),
)

pkg_tar(
    name = "heron-lib-uploader",
    package_dir = "lib/uploader",
    srcs = heron_client_lib_uploader_files(),
)

pkg_tar(
    name = "heron-lib-downloader",
    package_dir = "lib/downloaders",
    srcs = heron_client_lib_downloader_files(),
)

pkg_tar(
    name = "heron-lib-healthmgr",
    package_dir = "lib/healthmgr",
    srcs = heron_client_lib_healthmgr_files(),
)

pkg_tar(
    name = "heron-lib-third_party",
    package_dir = "lib/third_party",
    srcs = heron_client_lib_third_party_files(),
)

pkg_tar(
    name = "heron-lib-api",
    package_dir = "lib/api",
    srcs = heron_tools_lib_files(),
)

pkg_tar(
    name = "heron-examples",
    package_dir = "examples",
    srcs = heron_examples_files(),
)

pkg_tar(
    name = "heron-conf",
    strip_prefix = "/heron/config/src/yaml/conf",
    package_dir = "conf",
    srcs = [
        "//heron/config/src/yaml:conf-yaml",
    ],
)

pkg_tar(
    name = "heron-conf-tracker",
    strip_prefix = "/heron/tools/config/src/yaml/tracker",
    package_dir = "conf",
    srcs = heron_tools_conf_files(),
)

pkg_tar(
    name = "heron-dist",
    package_dir = "dist",
    srcs = [":heron-core"],
)

pkg_tar(
    name = "heron",
    extension = "tar.gz",
    srcs = generated_release_files,
    deps = [
        ":heron-bin",
        ":heron-conf",
        ":heron-conf-tracker",
        ":heron-dist",
        ":heron-examples",
        ":heron-lib-api",
        ":heron-lib-third_party",
        ":heron-lib-scheduler",
        ":heron-lib-packing",
        ":heron-lib-statemgr",
        ":heron-lib-metricscachemgr",
        ":heron-lib-uploader",
        ":heron-lib-downloader",
    ],
)

################################################################################
# Heron test packaging
################################################################################
pkg_tar(
    name = "heron-tests-bin",
    package_dir = "bin",
    srcs = [
        "//integration_test/src/python/http_server:http-server",
        "//integration_test/src/python/test_runner:test-runner",
    ]
)

pkg_tar(
    name = "heron-tests-data-scala",
    package_dir = "data/scala",
    srcs = [
        "//integration_test/src/scala:test-data-files",
    ],
    strip_prefix = '/integration_test/src/scala/org/apache/heron/integration_test/topology/'
)

pkg_tar(
    name = "heron-tests-data-java",
    package_dir = "data/java",
    srcs = [
        "//integration_test/src/java:test-data-files",
    ],
    strip_prefix = '/integration_test/src/java/org/apache/heron/integration_test/topology/'
)

pkg_tar(
    name = "heron-tests-data-python",
    package_dir = "data/python",
    srcs = [
        "//integration_test/src/python/integration_test/topology:test-data-files",
    ],
    strip_prefix = '/integration_test/src/python/integration_test/topology/'
)

pkg_tar(
    name = "heron-tests-lib",
    package_dir = "lib",
    srcs = [
       "//integration_test/src/scala:scala-integration-tests",
       "//integration_test/src/java:integration-tests",
       "//integration_test/src/python/integration_test/topology:heron_integ_topology",
    ],
)

pkg_tar(
    name = "heron-tests",
    extension = "tar.gz",
    srcs = generated_release_files,
    deps = [
        ":heron-tests-bin",
        ":heron-tests-data-scala",
        ":heron-tests-data-java",
        ":heron-tests-data-python",
        ":heron-tests-lib",
    ],
)

################################################################################
# Heron python packaging
#
# When a new python module is being added into the heronpy package, please ensure
# that its dependencies are being added into the packaging as well. Sometimes,
# the dependencies might already exist. For example,
#   heron/api depends on proto
#   heron/streamlet depends on proto and heron/api
#
# Hence the python package consists of the dependencies proto and heron/api.
# Note that the heron/api is exported as external API for heronpy and it is
# duplicated for dependency for heron/streamlet.
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
        "//heronpy/api:heron-python-package",
        "//heronpy/streamlet:heron-python-streamlet-api-package",
        "//heronpy/connectors:heron-pythonconnectors-package",
        "//heronpy/proto:proto-py-package",
    ],
    cmd = "\n".join([
        'export OUTPUT_DIR=$$(pwd)/$(@D)',
        'export RELEASE_FILE_DIR=$$(pwd)',
        'export TMP_DIR=$$(mktemp -d -t heronpy.XXXXX)',
        'echo $$TMP_DIR',
        'export HERONPY_DIR=$$TMP_DIR/heronpy',
        'export HERONPYAPI_UNZIP=$$TMP_DIR/heronapiunzip',
        'export HERONPYSTREAMLET_UNZIP=$$TMP_DIR/heronstreamletunzip',
        'export HERONPYCONNECTORS_UNZIP=$$TMP_DIR/heronconnectorsunzip',
        'export HERONPYPROTO_UNZIP=$$TMP_DIR/heronprotounzip',
        'export HERON_VERSION=$$(grep version $$RELEASE_FILE_DIR/$(location :release.yaml) | awk \'{print $$3}\')',
        'export HERON_VERSION=$$(echo $$HERON_VERSION | sed -e "s/^\'//" -e "s/\'$$//")',
        'export HERON_VERSION=$$(echo $$HERON_VERSION | grep "[0-9]*\.[0-9]*\.[0-9]*")',
        'export HERON_VERSION=$$([[ -z $$HERON_VERSION ]] && echo "0.0.0" || echo $$HERON_VERSION)',
        'echo $$HERON_VERSION',
        'mkdir -p $$TMP_DIR $$HERONPY_DIR',
        'unzip -qd $$HERONPYAPI_UNZIP $(location //heronpy/api:heron-python-package)',
        'unzip -qd $$HERONPYSTREAMLET_UNZIP $(location //heronpy/streamlet:heron-python-streamlet-api-package)',
        'unzip -qd $$HERONPYCONNECTORS_UNZIP $(location //heronpy/connectors:heron-pythonconnectors-package)',
        'unzip -qd $$HERONPYPROTO_UNZIP $(location //heronpy/proto:proto-py-package)',
        'find $$HERONPYAPI_UNZIP -name "*.pyc" -exec rm {} \;',
        'find $$HERONPYSTREAMLET_UNZIP -name "*.pyc" -exec rm {} \;',
        'find $$HERONPYCONNECTORS_UNZIP -name "*.pyc" -exec rm {} \;',
        'find $$HERONPYPROTO_UNZIP -name "*.pyc" -exec rm {} \;',
        'mkdir -p $$HERONPY_DIR/heronpy/api',
        'mkdir -p $$HERONPY_DIR/heronpy/streamlet',
        'mkdir -p $$HERONPY_DIR/heronpy/connectors',
        'mkdir -p $$HERONPY_DIR/heronpy/proto',
        'mv $$HERONPYAPI_UNZIP/heronpy/api $$HERONPY_DIR/heronpy/',
        'mv $$HERONPYSTREAMLET_UNZIP/heronpy/streamlet $$HERONPY_DIR/heronpy/',
        'mv $$HERONPYCONNECTORS_UNZIP/heronpy/connectors $$HERONPY_DIR/heronpy/',
        'mv $$HERONPYPROTO_UNZIP/heronpy/proto $$HERONPY_DIR/heronpy/',
        'echo "__import__(\'pkg_resources\').declare_namespace(__name__)" > $$HERONPY_DIR/heronpy/__init__.py',
        'rm -rf $$HERONPYAPI_UNZIP/heron/api',
        'rm -rf $$HERONPYSTREAMLET_UNZIP/heron/streamlet',
        'rm -rf $$HERONPYCONNECTORS_UNZIP/heron/connectors',
        'rm -rf $$HERONPYPROTO_UNZIP/heron/proto',
        'cp -R $(SRCS) $$HERONPY_DIR',
        'echo "Heron Python toplevel directory: $$HERONPY_DIR"',
        'cd $$HERONPY_DIR',
        'sed "s/VERSION/$$HERON_VERSION/" setup.py.template > setup.py',
        'find heronpy -type f -name "*.bak" -delete',
        'rm setup.py.template',
        'tree $$HERONPY_DIR',
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
# Heron helm chart packaging
################################################################################
genrule(
    name = "generate-helm-charts",
    srcs = [
        "//deploy/kubernetes/helm:chart"
    ] + generated_release_files,
    outs = ['index.yaml'],
    tools = [
        "//third_party/helm:helm",
    ],
    stamp = 1,
    cmd = "\n".join([
        'export OUTPUT_DIR=$$(pwd)/$(@D)',
        'export RELEASE_FILE_DIR=$$(pwd)',
        'export TMP_DIR=$$(mktemp -d -t helm.XXXXX)',
        'export HELM_DIR=$$TMP_DIR/heron',
        'export HERON_VERSION=$$(grep version $$RELEASE_FILE_DIR/$(location :release.yaml) | awk \'{print $$3}\')',
        'export HERON_VERSION=$$(echo $$HERON_VERSION | sed -e "s/^\'//" -e "s/\'$$//")',
        'export HERON_VERSION=$$(echo $$HERON_VERSION | grep "[0-9]*\.[0-9]*\.[0-9]*")',
        'export HERON_VERSION=$$([[ -z $$HERON_VERSION ]] && echo "0.0.0" || echo $$HERON_VERSION)',
        'mkdir -p $$TMP_DIR $$HELM_DIR heron-charts',
        'cp $(SRCS) $$HELM_DIR',
        'sed "s/VERSION/$$HERON_VERSION/" $$HELM_DIR/Chart.yaml.template > $$HELM_DIR/Chart.yaml',
        'sed "s/VERSION/$$HERON_VERSION/" $$HELM_DIR/values.yaml.template > $$HELM_DIR/values.yaml',
        'mkdir $$HELM_DIR/charts $$HELM_DIR/templates',
        'mv $$HELM_DIR/bookie.yaml $$HELM_DIR/tools.yaml $$HELM_DIR/templates',
        'mv $$HELM_DIR/zookeeper.yaml $$HELM_DIR/templates',
        'mv $$HELM_DIR/*.txt $$HELM_DIR/*.tpl $$HELM_DIR/templates',
        'rm $$HELM_DIR/Chart.yaml.template $$HELM_DIR/values.yaml.template $$HELM_DIR/release.yaml',
        'tree $$HELM_DIR',
        '$(location //third_party/helm:helm) init --client-only',
        '$(location //third_party/helm:helm) package $$HELM_DIR',
        'mv *.tgz heron-charts',
        '$(location //third_party/helm:helm) repo index heron-charts --url https://storage.googleapis.com/heron-charts',
        'cp heron-charts/* $$OUTPUT_DIR',
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
    name = "generate-launcher",
    srcs = ["template_bin.sh", "bin_common.sh"] + generated_release_files,
    outs = ["launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location bin_common.sh))"
        template="$$(cat $(location template_bin.sh))"
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

load("//scripts/packages:self_extract_binary.bzl", "self_extract_binary")

self_extract_binary(
    name = "heron-install.sh",
    flatten_resources = [
        ":heron",
    ],
    launcher = ":launcher_bin.sh",
)

self_extract_binary(
    name = "heron-tests-install.sh",
    flatten_resources = [
        ":heron-tests",
    ],
    launcher = ":tests_launcher_bin.sh",
)

filegroup(
    name = "template-bin.sh",
    srcs = ["template_bin.sh"],
)

filegroup(
    name = "tests-template-bin.sh",
    srcs = ["tests_template_bin.sh"],
)

filegroup(
    name = "bin-common.sh",
    srcs = ["bin_common.sh"],
)
