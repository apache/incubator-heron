package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_binary")

release_version = "unversioned"

filegroup(
    name = "packages",
    srcs = [
        ":api",
        ":core",
        ":cli",
        ":metrics-api",
        ":storm-compat",
        ":tracker",
        ":viz",
    ],
)

pex_binary(
    name = "dorelease",
    srcs = ["dorelease.py"],
    eggs = [
        "//3rdparty/eggs:async",
        "//3rdparty/eggs:pygit",
        "//3rdparty/eggs:gitdb",
        "//3rdparty/eggs:pyyaml",
        "//3rdparty/eggs:thrift",
        "//3rdparty/eggs:smmap",
    ],
)

genrule(
    name = "version",
    outs = [
        "RELEASE"
    ],
    cmd = "\n".join([
        "export INSTALL_DIR=$$(pwd)/$(@D)",
        "echo $(RELEASE) > $$INSTALL_DIR/RELEASE"
    ]),
)

genrule(
    name = "core",
    srcs = [
        ":version",
        ":htmaster",
        ":hstmgr",
        ":hinstance",
        ":hlogging",
        ":hscheduler",
        ":hmetricsmgr",
        ":hmetrics-sinks",
        ":hexecutor",
        ":hshell",
        ":hcontroller",
    ],
    outs = [
        "heron-core-" + release_version + ".tar",
        "heron-core-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-core-" + release_version + ".tar)",
        "--cp $(location version)        RELEASE",
        "--cp $(location htmaster)       heron-tmaster",
        "--cp $(location hstmgr)         heron-stmgr",
        "--cp $(location hinstance)      heron-instance.jar",
        "--cp $(location hscheduler)     heron-scheduler.jar",
        "--cp $(location hmetricsmgr)    metrics-mgr-classpath/heron-metricsmgr.jar",
        "--cp $(location hmetrics-sinks) metrics_sinks.yaml",
        "--cp $(location hcontroller)    heron-controller",
        "--cp $(location hexecutor)      heron-executor",
        "--cp $(location hshell)         heron-shell",
        "--cp $(location hlogging)       aurora_logging.properties",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

genrule(
    name = "cli",
    srcs = [
        ":version",
        ":hcli",
        ":haurora-job",
        ":hcontroller-job",
        ":hinternals-config",
        ":hscheduler",
        ":hcli2",
        ":haurora-scheduler-config",
        ":protobuf-java",
        ":slf4j-api-java",
        ":slf4j-jdk-java",
        ":log4j-over-slf4j-java",
    ],
    outs = [
        "heron-cli-" + release_version + ".tar",
        "heron-cli-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-cli-" + release_version + ".tar)",
        "--cp $(location version)                  RELEASE",
        "--cp $(location hcli)                     bin/heron-cli",
        "--cp $(location haurora-job)              bin/heron.aurora",
        "--cp $(location hcontroller-job)          bin/heron_controller.aurora",
        "--cp $(location hinternals-config)        heron_internals.yaml",
        "--cp $(location hscheduler)               lib/heron-scheduler.jar",
        "--cp $(location hcli2)                    bin/heron-cli2",
        "--cp $(location haurora-scheduler-config) scheduler.conf",
        "--cp $(location protobuf-java)            lib/$$(basename $(location protobuf-java))",
        "--cp $(location slf4j-api-java)           lib/$$(basename $(location slf4j-api-java))",
        "--cp $(location slf4j-jdk-java)           lib/$$(basename $(location slf4j-jdk-java))",
        "--cp $(location log4j-over-slf4j-java)    lib/$$(basename $(location log4j-over-slf4j-java))",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

genrule(
    name = "api",
    srcs = [
        ":version",
        ":hapi",
    ],
    outs = [
        "heron-api-" + release_version + ".tar",
        "heron-api-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-api-" + release_version + ".tar)",
        "--cp $(location version)                  RELEASE",
        "--cp $(location hapi)                     heron-api.jar",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

genrule(
    name = "metrics-api",
    srcs = [
        ":version",
        ":hmetrics-api",
    ],
    outs = [
        "heron-metrics-api-" + release_version + ".tar",
        "heron-metrics-api-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-metrics-api-" + release_version + ".tar)",
        "--cp $(location version)                  RELEASE",
        "--cp $(location hmetrics-api)             heron-metricsmgr-api.jar",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

genrule(
    name = "storm-compat",
    srcs = [
        ":version",
        ":hstorm-compat",
    ],
    outs = [
        "heron-storm-compat-" + release_version + ".tar",
        "heron-storm-compat-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-storm-compat-" + release_version + ".tar)",
        "--cp $(location hstorm-compat)            heron-storm-compatibility.jar",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

genrule(
    name = "tracker",
    srcs = [
        ":version",
        ":htracker",
    ],
    outs = [
        "heron-tracker-" + release_version + ".tar",
        "heron-tracker-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-tracker-" + release_version + ".tar)",
        "--cp $(location version)                  RELEASE",
        "--cp $(location htracker)                 heron-tracker",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

genrule(
    name = "viz",
    srcs = [
        ":version",
        ":hviz",
    ],
    outs = [
        "heron-viz-" + release_version + ".tar",
        "heron-viz-" + release_version + ".tar.gz",
    ],
    cmd = " ".join([
        "export GENDIR=$(GENDIR);",
        "export BINDIR=$(BINDIR);",
        "$(location package_release.sh) $(location heron-viz-" + release_version + ".tar)",
        "--cp $(location version)                  RELEASE",
        "--cp $(location hviz)                     heron-viz",
    ]),
    heuristic_label_expansion = False,
    tags = ["manual"],
    tools = ["package_release.sh"],
)

filegroup(
    name = "htmaster",
    srcs = ["//heron/tmaster/src/cpp:heron-tmaster"],
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
    srcs = ["//heron/scheduler/src/java:heron-scheduler_deploy.jar"],
)

filegroup(
    name = "hmetricsmgr",
    srcs = ["//heron/metricsmgr/src/java:heron-metricsmgr_deploy.jar"],
)

filegroup(
    name = "hmetrics-sinks",
    srcs = ["//heron/config:metrics-sinks-yaml"],
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
    name = "hcontroller",
    srcs = ["//heron/controller/src/python:heron-controller"],
)

filegroup(
    name = "hcli",
    srcs = ["//heron/cli/src/python:heron-cli"],
)

filegroup(
    name = "haurora-job",
    srcs = ["//heron/cli/src/python:heron-aurora"],
)

filegroup(
    name = "hcontroller-job",
    srcs = ["//heron/cli/src/python:heron-controller-aurora"],
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
    name = "protobuf-java",
    srcs = ["//3rdparty/protobuf:protobuf-java"],
)

filegroup(
    name = "slf4j-api-java",
    srcs = ["//3rdparty/logging:slf4j-api-java"],
)

filegroup(
    name = "slf4j-jdk-java",
    srcs = ["//3rdparty/logging:slf4j-jdk-java"],
)

filegroup(
    name = "log4j-over-slf4j-java",
    srcs = ["//3rdparty/logging:log4j-over-slf4j-java"],
)

filegroup(
    name = "hapi",
    srcs = ["//heron/api/src/java:api-java"],
)

filegroup(
    name = "hmetrics-api",
    srcs = ["//heron/metricsmgr-api/src/java:metricsmgr-api-java"],
)

filegroup(
    name = "hstorm-compat",
    srcs = ["//heron/storm/src/java:storm-compatibility-java"],
)

filegroup(
    name = "hviz",
    srcs = ["//heron/viz/src/python:heron-viz"],
)

filegroup(
    name = "htracker",
    srcs = ["//heron/tracker/src/python:heron-tracker"],
)
