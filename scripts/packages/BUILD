package(default_visibility = ["//visibility:public"])

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar", "pkg_deb")

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
    srcs = [
        "//heron/executor/src/python:heron-executor",
        "//heron/downloaders/src/shell:heron-downloader",
        "//heron/downloaders/src/shell:heron-downloader-config",
        "//heron/instance/src/python:heron-python-instance",
        "//heron/instance/src/cpp:heron-cpp-instance",
        "//heron/shell/src/python:heron-shell",
        "//heron/stmgr/src/cpp:heron-stmgr",
        "//heron/tmaster/src/cpp:heron-tmaster",
    ],
)

pkg_tar(
    name = "heron-core-lib-scheduler",
    package_dir = "heron-core/lib/scheduler",
    srcs = [
        "//heron/scheduler-core/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-kubernetes-scheduler",
        "//heron/schedulers/src/java:heron-marathon-scheduler",
        "//heron/schedulers/src/java:heron-mesos-scheduler",
        "//heron/schedulers/src/java:heron-nomad-scheduler",
        "//heron/schedulers/src/java:heron-slurm-scheduler",
    ],
)

pkg_tar(
    name = "heron-core-lib-packing",
    package_dir = "heron-core/lib/packing",
    srcs = [ 
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//heron/packing/src/java:heron-binpacking-packing"
    ],
)

pkg_tar(
    name = "heron-core-lib-healthmgr",
    package_dir = "heron-core/lib/healthmgr",
    srcs = [
        "//heron/healthmgr/src/java:heron-healthmgr",
    ],
)

pkg_tar(
    name = "heron-core-lib-metricsmgr",
    package_dir = "heron-core/lib/metricsmgr",
    srcs = [
        "//heron/metricsmgr/src/java:heron-metricsmgr",
    ],
)

pkg_tar(
    name = "heron-core-lib-metricscachemgr",
    package_dir = "heron-core/lib/metricscachemgr",
    srcs = [
        "//heron/metricscachemgr/src/java:heron-metricscachemgr",
    ],
)

pkg_tar(
    name = "heron-core-lib-statemgr",
    package_dir = "heron-core/lib/statemgr",
    srcs = [
        "//heron/statemgrs/src/java:heron-localfs-statemgr",
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
    ],
)

pkg_tar(
    name = "heron-core-lib-instance",
    package_dir = "heron-core/lib/instance",
    srcs = [
        "//heron/instance/src/java:heron-instance",
    ],
)

pkg_tar(
    name = "heron-core-lib-ckptmgr",
    package_dir = "heron-core/lib/ckptmgr",
    srcs = [
        "//heron/ckptmgr/src/java:heron-ckptmgr",
    ],
)

pkg_tar(
    name = "heron-core-lib-statefulstorage",
    package_dir = "heron-core/lib/statefulstorage",
    srcs = [
        "//heron/statefulstorages/src/java:heron-localfs-statefulstorage",
        "//heron/statefulstorages/src/java:heron-hdfs-statefulstorage",
        "//heron/statefulstorages/src/java:heron-dlog-statefulstorage",
    ],
)

pkg_tar(
    name = "heron-core-lib-downloader",
    package_dir = "heron-core/lib/downloaders",
    srcs = [
        "//heron/downloaders/src/java:heron-downloader",
    ],
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
    srcs = [
        "//heron/tools/apiserver/src/shell:heron-apiserver",
        "//heron/tools/cli/src/python:heron",
        "//heron/tools/explorer/src/python:heron-explorer",
        "//heron/tools/tracker/src/python:heron-tracker",
        "//heron/tools/ui/src/python:heron-ui",
    ],
)

pkg_tar(
    name = "heron-tools-lib-api",
    package_dir = "heron-tools/lib/api",
    srcs = [
        "//heron/tools/apiserver/src/java:heron-apiserver",
    ],
)

pkg_tar(
    name = "heron-tools-lib-third_party",
    package_dir = "heron-tools/lib/third_party",
    srcs = [
        "@com_google_protobuf//:protobuf_java",
        "@org_slf4j_slf4j_api//jar",
        "@org_slf4j_slf4j_jdk14//jar",
    ],
)

pkg_tar(
    name = "heron-tools-lib-uploader",
    package_dir = "heron-tools/lib/uploader",
    srcs = [
        "//heron/uploaders/src/java:heron-null-uploader",
        "//heron/uploaders/src/java:heron-localfs-uploader",
        "//heron/uploaders/src/java:heron-s3-uploader",
        "//heron/uploaders/src/java:heron-hdfs-uploader",
        "//heron/uploaders/src/java:heron-scp-uploader",
        "//heron/uploaders/src/java:heron-gcs-uploader",
        "//heron/uploaders/src/java:heron-dlog-uploader",
        "//heron/uploaders/src/java:heron-http-uploader",
    ],
)

pkg_tar(
    name = "heron-tools-conf",
    package_dir = "heron-tools/conf",
    srcs = [
        "//heron/tools/config/src/yaml:tracker-yaml",
    ],
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
    srcs = [
        "//heron/downloaders/src/shell:heron-downloader",
        "//heron/downloaders/src/shell:heron-downloader-config",
        "//heron/tools/cli/src/python:heron",
        "//heron/tools/explorer/src/python:heron-explorer",
        "//heron/tools/admin/src/python:heron-admin",
        "//heron/tools/tracker/src/python:heron-tracker",
        "//heron/tools/ui/src/python:heron-ui",
        "//heron/tools/apiserver/src/shell:heron-apiserver",
        "//third_party/nomad:heron-nomad",
    ],
)

pkg_tar(
    name = "heron-include-bolt",
    package_dir = "include/bolt",
    srcs = ["//heron/api/src/cpp:cxx-bolt-hdrs"],
)

pkg_tar(
    name = "heron-include-config",
    package_dir = "include/config",
    srcs = ["//heron/api/src/cpp:cxx-config-hdrs"],
)

pkg_tar(
    name = "heron-include-exceptions",
    package_dir = "include/exceptions",
    srcs = ["//heron/api/src/cpp:cxx-exceptions-hdrs"],
)

pkg_tar(
    name = "heron-include-metric",
    package_dir = "include/metric",
    srcs = ["//heron/api/src/cpp:cxx-metric-hdrs"],
)

pkg_tar(
    name = "heron-include-serializer",
    package_dir = "include/serializer",
    srcs = ["//heron/api/src/cpp:cxx-serializer-hdrs"],
)

pkg_tar(
    name = "heron-include-spout",
    package_dir = "include/spout",
    srcs = ["//heron/api/src/cpp:cxx-spout-hdrs"],
)

pkg_tar(
    name = "heron-include-topology",
    package_dir = "include/topology",
    srcs = ["//heron/api/src/cpp:cxx-topology-hdrs"],
)

pkg_tar(
    name = "heron-include-tuple",
    package_dir = "include/tuple",
    srcs = ["//heron/api/src/cpp:cxx-tuple-hdrs"],
)

pkg_tar(
    name = "heron-include-utils",
    package_dir = "include/utils",
    srcs = ["//heron/api/src/cpp:cxx-utils-hdrs"],
)

pkg_tar(
    name = "heron-lib-scheduler",
    package_dir = "lib/scheduler",
    srcs = [
        "//heron/scheduler-core/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-aurora-scheduler",
        "//heron/schedulers/src/java:heron-slurm-scheduler",
        "//heron/schedulers/src/java:heron-yarn-scheduler",
        "//heron/schedulers/src/java:heron-mesos-scheduler",
        "//heron/schedulers/src/java:heron-marathon-scheduler",
        "//heron/schedulers/src/java:heron-kubernetes-scheduler",
        "//heron/schedulers/src/java:heron-nomad-scheduler",
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//heron/packing/src/java:heron-binpacking-packing",
    ],
)

pkg_tar(
    name = "heron-lib-metricscachemgr",
    package_dir = "lib/metricscachemgr",
    srcs = [
        "//heron/metricscachemgr/src/java:heron-metricscachemgr",
    ],
)

pkg_tar(
    name = "heron-lib-packing",
    package_dir = "lib/packing",
    srcs = [
       "//heron/packing/src/java:heron-roundrobin-packing",
       "//heron/packing/src/java:heron-binpacking-packing",
    ],
)

pkg_tar(
    name = "heron-lib-statemgr",
    package_dir = "lib/statemgr",
    srcs = [
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
        "//heron/statemgrs/src/java:heron-localfs-statemgr",
    ],
)

pkg_tar(
    name = "heron-lib-uploader",
    package_dir = "lib/uploader",
    srcs = [
        "//heron/uploaders/src/java:heron-null-uploader",
        "//heron/uploaders/src/java:heron-localfs-uploader",
        "//heron/uploaders/src/java:heron-s3-uploader",
        "//heron/uploaders/src/java:heron-hdfs-uploader",
        "//heron/uploaders/src/java:heron-scp-uploader",
        "//heron/uploaders/src/java:heron-gcs-uploader",
        "//heron/uploaders/src/java:heron-dlog-uploader",
        "//heron/uploaders/src/java:heron-http-uploader"
    ],
)

pkg_tar(
    name = "heron-lib-downloader",
    package_dir = "lib/downloaders",
    srcs = [
        "//heron/downloaders/src/java:heron-downloader",
    ],
)

pkg_tar(
    name = "heron-lib-healthmgr",
    package_dir = "lib/healthmgr",
    srcs = [
        "//heron/healthmgr/src/java:heron-healthmgr",
    ],
)

pkg_tar(
    name = "heron-lib-third_party",
    package_dir = "lib/third_party",
    srcs = [
        "@com_google_protobuf//:protobuf_java",
        "@org_slf4j_slf4j_api//jar",
        "@org_slf4j_slf4j_jdk14//jar",
    ],
)

pkg_tar(
    name = "heron-lib-api",
    package_dir = "lib/api",
    srcs = [
        "//heron/api/src/cpp:cxx-api",
        "//heron/api/src/java:api-java",
        "//heron/api/src/scala:api-scala",
        "//heron/tools/apiserver/src/java:heron-apiserver",
    ],
)

pkg_tar(
    name = "heron-examples",
    package_dir = "examples",
    srcs = [
        "//eco-storm-examples/src/java:storm-eco-examples",
        "//eco-storm-examples/src/java:storm-eco-examples-support",
        "//eco-heron-examples/src/java:heron-eco-examples",
        "//eco-heron-examples/src/java:heron-eco-examples-support",
        "//examples/src/java:heron-api-examples",
        "//examples/src/java:heron-streamlet-examples",
        "//examples/src/scala:heron-streamlet-scala-examples",
    ],
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
    srcs = [
        "//heron/tools/config/src/yaml:tracker-yaml",
    ],
)

pkg_tar(
    name = "heron-dist",
    package_dir = "dist",
    srcs = [":heron-core"],
)

pkg_tar(
    name = "heron-simulator",
    package_dir = "lib/simulator",
    srcs = [
        "//heron/simulator/src/java:simulator-java",
    ],
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
        ":heron-include-bolt",
        ":heron-include-config",
        ":heron-include-exceptions",
        ":heron-include-metric",
        ":heron-include-serializer",
        ":heron-include-spout",
        ":heron-include-topology",
        ":heron-include-tuple",
        ":heron-include-utils",
        ":heron-lib-api",
        ":heron-lib-downloader",
        ":heron-lib-metricscachemgr",
        ":heron-lib-packing",
        ":heron-lib-scheduler",
        ":heron-lib-statemgr",
        ":heron-lib-third_party",
        ":heron-lib-uploader",
        ":heron-simulator",
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
        "//integration_test/src/python/topology_test_runner:topology-test-runner",
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
    name = "heron-tests-data-topology-java",
    package_dir = "data/java/topology_test",
    srcs = [
        "//integration_test/src/java:topology-test-data-files",
    ],
    strip_prefix = '/integration_test/src/java/org/apache/heron/integration_topology_test/topology/'
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
       "//integration_test/src/java:integration-topology-tests",
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
        ":heron-tests-data-topology-java",
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
        '/usr/bin/env python2.7 setup.py bdist_wheel --universal',
        'cd -',
        'ls -l $$HERONPY_DIR/dist',
        'cp $$HERONPY_DIR/dist/heronpy-*-py2.py3-*.whl $$OUTPUT_DIR',
        'cp $$HERONPY_DIR/dist/heronpy-*-py2.py3-*.whl "$@"',
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
