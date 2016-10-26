package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_binary")

release_version = "unversioned"

filegroup(
    name = "tarpkgs",
    srcs = [
        ":heron-api",
        ":heron-core",
        ":heron-client",
        ":heron-tools",
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

################################################################################
# Heron core packaging
################################################################################
genrule(
    name = "heron-core",
    srcs = [
        ":release.yaml",
        ":hexecutor",
        ":hshell",
        ":hstmgr",
        ":htmaster",
        ":hpyinstance",
        ":hscheduler",
        ":hscheduler-aurora",
        ":hscheduler-local",
        ":hbppacking",
        ":hrrpacking",
        ":hmetricsmgr",
        ":hlfsstatemgr",
        ":hzkstatemgr",
        ":hinstance",
    ],
    outs = [
        "heron-core.tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh)            $(location heron-core.tar.gz)",
        "--cp $(location release.yaml)             heron-core/release.yaml",
        "--cp $(location hexecutor)                heron-core/bin/heron-executor",
        "--cp $(location hshell)                   heron-core/bin/heron-shell",
        "--cp $(location hstmgr)                   heron-core/bin/heron-stmgr",
        "--cp $(location htmaster)                 heron-core/bin/heron-tmaster",
        "--cp $(location hpyinstance)              heron-core/bin/heron-python-instance",
        "--cp $(location hscheduler)               heron-core/lib/scheduler/heron-scheduler.jar",
        "--cp $(location hscheduler-aurora)        heron-core/lib/scheduler/heron-aurora-scheduler.jar",
        "--cp $(location hscheduler-local)         heron-core/lib/scheduler/heron-local-scheduler.jar",
        "--cp $(location hbppacking)               heron-core/lib/packing/heron-binpacking-packing.jar",
        "--cp $(location hrrpacking)               heron-core/lib/packing/heron-roundrobin-packing.jar",
        "--cp $(location hmetricsmgr)              heron-core/lib/metricsmgr/heron-metricsmgr.jar",
        "--cp $(location hlfsstatemgr)             heron-core/lib/statemgr/heron-localfs-statemgr.jar",
        "--cp $(location hzkstatemgr)              heron-core/lib/statemgr/heron-zookeeper-statemgr.jar",
        "--cp $(location hinstance)                heron-core/lib/instance/heron-instance.jar",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

################################################################################
# Heron client packaging
################################################################################
genrule(
    name = "heron-client",
    srcs = [
        ":release.yaml",
        ":heron-core",
        ":conf-local-heron-internals",
        ":conf-local-metrics-sinks",
        ":conf-local-client",
        ":conf-local-packing",
        ":conf-local-scheduler",
        ":conf-local-statemgr",
        ":conf-local-uploader",
        ":hcli",
        ":hexamples",
        ":hscheduler",
        ":hscheduler-aurora",
        ":hscheduler-local",
        ":hbppacking",
        ":hrrpacking",
        ":hlfsstatemgr",
        ":hzkstatemgr",
        ":huploader-localfs",
        ":huploader-null",
        ":protobuf-java",
        ":slf4j-api-java",
        ":slf4j-jdk-java",
    ],
    outs = [
        "heron-client.tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-client.tar.gz)",
        "--cp $(location release.yaml)               release.yaml",
        "--cp $(location hcli)                       bin/heron",
        "--cp $(location conf-local-heron-internals) conf/local/heron_internals.yaml",
        "--cp $(location conf-local-metrics-sinks)   conf/local/metrics_sinks.yaml",
        "--cp $(location conf-local-client)          conf/local/client.yaml",
        "--cp $(location conf-local-packing)         conf/local/packing.yaml",
        "--cp $(location conf-local-scheduler)       conf/local/scheduler.yaml",
        "--cp $(location conf-local-statemgr)        conf/local/statemgr.yaml",
        "--cp $(location conf-local-uploader)        conf/local/uploader.yaml",
        "--cp $(location heron-core)                 dist/heron-core.tar.gz",
        "--cp $(location hexamples)                  examples/heron-examples.jar",
        "--cp $(location hscheduler)                 lib/scheduler/heron-scheduler.jar",
        "--cp $(location hscheduler-aurora)          lib/scheduler/heron-aurora-scheduler.jar",
        "--cp $(location hscheduler-local)           lib/scheduler/heron-local-scheduler.jar",
        "--cp $(location hbppacking)                 lib/packing/heron-binpacking-packing.jar",
        "--cp $(location hrrpacking)                 lib/packing/heron-roundrobin-packing.jar",
        "--cp $(location hlfsstatemgr)               lib/statemgr/heron-localfs-statemgr.jar",
        "--cp $(location hzkstatemgr)                lib/statemgr/heron-zookeeper-statemgr.jar",
        "--cp $(location huploader-localfs)          lib/uploader/heron-localfs-uploader.jar",
        "--cp $(location huploader-null)             lib/uploader/heron-null-uploader.jar",
        "--cp $(location protobuf-java)              lib/third_party/$$(basename $(location protobuf-java))",
        "--cp $(location slf4j-api-java)             lib/third_party/$$(basename $(location slf4j-api-java))",
        "--cp $(location slf4j-jdk-java)             lib/third_party/$$(basename $(location slf4j-jdk-java))",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

################################################################################
# Heron tools packaging
################################################################################
genrule(
    name = "heron-tools",
    srcs = [
        ":release.yaml",
        ":htracker",
        ":hui",
    ],
    outs = [
        "heron-tools.tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-tools.tar.gz)",
        "--cp $(location release.yaml)               release.yaml",
        "--cp $(location htracker)                   bin/heron-tracker",
        "--cp $(location hui)                        bin/heron-ui",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

################################################################################
# Heron api packaging
################################################################################
genrule(
    name = "heron-api",
    srcs = [
        ":release.yaml",
        ":hapi",
        ":hspi",
        ":hstorm",
    ],
    outs = [
        "heron-api.tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh)              $(location heron-api.tar.gz)",
        "--cp $(location release.yaml)               release.yaml",
        "--cp $(location hapi)                       heron-api.jar",
        "--cp $(location hspi)                       heron-spi.jar",
        "--cp $(location hstorm)                     heron-storm.jar",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

filegroup(
    name = "conf-local-heron-internals",
    srcs = ["//heron/tools/config/src/yaml:conf-local-heron-internals"],
)

filegroup(
    name = "conf-local-metrics-sinks",
    srcs = ["//heron/tools/config/src/yaml:conf-local-metrics-sinks"],
)

filegroup(
    name = "conf-local-client",
    srcs = ["//heron/tools/config/src/yaml:conf-local-client"],
)

filegroup(
    name = "conf-local-packing",
    srcs = ["//heron/tools/config/src/yaml:conf-local-packing"],
)

filegroup(
    name = "conf-local-scheduler",
    srcs = ["//heron/tools/config/src/yaml:conf-local-scheduler"],
)

filegroup(
    name = "conf-local-statemgr",
    srcs = ["//heron/tools/config/src/yaml:conf-local-statemgr"],
)

filegroup(
    name = "conf-local-uploader",
    srcs = ["//heron/tools/config/src/yaml:conf-local-uploader"],
)

filegroup(
    name = "hexamples",
    srcs = ["//heron/examples/src/java:heron-examples"],
)

filegroup(
    name = "htmaster",
    srcs = ["//heron/tmaster/src/cpp:heron-tmaster"],
)

filegroup(
    name = "hpyinstance",
    srcs = ["//heron/instance/src/python/instance:heron-python-instance"],
)

filegroup(
    name = "hstmgr",
    srcs = ["//heron/stmgr/src/cpp:heron-stmgr"],
)

filegroup(
    name = "hinstance",
    srcs = ["//heron/instance/src/java:heron-instance"],
)

filegroup(
    name = "hlogging",
    srcs = ["//heron/instance/src/java:aurora-logging-properties"],
)

filegroup(
    name = "hscheduler",
    srcs = ["//heron/scheduler-core/src/java:heron-scheduler"],
)

filegroup(
    name = "hscheduler-aurora",
    srcs = ["//heron/schedulers/src/java:heron-aurora-scheduler"],
)

filegroup(
    name = "hscheduler-local",
    srcs = ["//heron/schedulers/src/java:heron-local-scheduler"],
)

filegroup(
    name = "hbppacking",
    srcs = ["//heron/packing/src/java:heron-binpacking-packing"],
)

filegroup(
    name = "hrrpacking",
    srcs = ["//heron/packing/src/java:heron-roundrobin-packing"],
)

filegroup(
    name = "hmetricsmgr",
    srcs = ["//heron/metricsmgr/src/java:heron-metricsmgr"],
)

filegroup(
    name = "hexecutor",
    srcs = ["//heron/executor/src/python:heron-executor"],
)

filegroup(
    name = "hshell",
    srcs = ["//heron/shell/src/python:heron-shell"],
)

filegroup(
    name = "hcli",
    srcs = ["//heron/tools/cli/src/python:heron"],
)

filegroup(
    name = "haurora-job",
    srcs = ["//heron/tools/cli/src/python:heron-aurora"],
)

filegroup(
    name = "hinternals-config",
    srcs = ["//heron/config:config-internals-yaml"],
)

filegroup(
    name = "hcli2",
    srcs = ["//heron/cli2/src/python:heron-cli2"],
)

filegroup(
    name = "hscheduler-config",
    srcs = ["//heron/cli2/src/python:scheduler-config"],
)

filegroup(
    name = "haurora-scheduler-config",
    srcs = ["//heron/cli2/src/python:aurora-scheduler-config"],
)

filegroup(
    name = "hlocal-scheduler-config",
    srcs = ["//heron/cli2/src/python:local-scheduler-config"],
)

filegroup(
    name = "hmesos-scheduler-config",
    srcs = ["//heron/cli2/src/python:mesos-scheduler-config"],
)

filegroup(
    name = "hlfsstatemgr",
    srcs = ["//heron/statemgrs/src/java:heron-localfs-statemgr"],
)

filegroup(
    name = "hzkstatemgr",
    srcs = ["//heron/statemgrs/src/java:heron-zookeeper-statemgr"],
)

filegroup(
    name = "protobuf-java",
    srcs = ["@com_google_protobuf_protobuf_java//jar"],
)

filegroup(
    name = "slf4j-api-java",
    srcs = ["@org_slf4j_slf4j_api//jar"],
)

filegroup(
    name = "slf4j-jdk-java",
    srcs = ["@org_slf4j_slf4j_jdk14//jar"],
)

filegroup(
    name = "hapi",
    srcs = ["//heron/api/src/java:heron-api"],
)

filegroup(
    name = "hspi",
    srcs = ["//heron/spi/src/java:heron-spi"],
)

filegroup(
    name = "hmetrics-api",
    srcs = ["//heron/metricsmgr-api/src/java:metricsmgr-api-java"],
)

filegroup(
    name = "hstorm",
    srcs = ["//heron/storm/src/java:heron-storm"],
)

filegroup(
    name = "hviz",
    srcs = ["//heron/viz/src/python:heron-viz"],
)

filegroup(
    name = "htracker",
    srcs = ["//heron/tools/tracker/src/python:heron-tracker"],
)

filegroup(
    name = "hui",
    srcs = ["//heron/tools/ui/src/python:heron-ui"],
)

filegroup(
    name = "huploader-localfs",
    srcs = ["//heron/uploaders/src/java:heron-localfs-uploader"],
)

filegroup(
    name = "huploader-null",
    srcs = ["//heron/uploaders/src/java:heron-null-uploader"],
)

genrule(
    name = "generate-package-info",
    outs = ["release.yaml"],
    cmd = "$(location //scripts/packages:package-info-generator) $$(find . -name '*status*.txt') >$@",
    stamp = 1,
    tools = ["//scripts/packages:package-info-generator"],
)

genrule(
    name = "generate-api-launcher",
    srcs = [
        ":release.yaml",
        "//scripts/packages:api-template-bin.sh",
        "//scripts/packages:bin-common.sh"
    ],
    outs = ["api_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location //scripts/packages:bin-common.sh))"
        template="$$(cat $(location //scripts/packages:api-template-bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

genrule(
    name = "generate-client-launcher",
    srcs = [
        ":release.yaml",
        "//scripts/packages:client-template-bin.sh",
        "//scripts/packages:bin-common.sh"
    ],
    outs = ["client_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location //scripts/packages:bin-common.sh))"
        template="$$(cat $(location //scripts/packages:client-template-bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

genrule(
    name = "generate-tools-launcher",
    srcs = [
        ":release.yaml",
        "//scripts/packages:tools-template-bin.sh",
        "//scripts/packages:bin-common.sh"
    ],
    outs = ["tools_launcher_bin.sh"],
    cmd = """
        release_info="$$(cat $(location :release.yaml))"
        bin_common="$$(cat $(location //scripts/packages:bin-common.sh))"
        template="$$(cat $(location //scripts/packages:tools-template-bin.sh))"
        echo "$${bin_common}\n\n$${template//%release_info%/$${release_info}}" >$@
        """,
)

load("/scripts/packages/self_extract_binary", "self_extract_binary")

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
