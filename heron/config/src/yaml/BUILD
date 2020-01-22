licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "test-config-internals-yaml",
    srcs = ["conf/test/test_heron_internals.yaml"],
)

filegroup(
    name = "test-config-metrics-sinks-yaml",
    srcs = ["conf/test/test_metrics_sinks.yaml"],
)

filegroup(
    name = "conf-yaml",
    srcs = glob(
        ["conf/**/*.aurora"] + [
            "conf/**/*.hcl",
        ] + [
            "conf/**/*.sh",
        ] + [
            "conf/**/*.yaml",
        ],
    ),
)

filegroup(
    name = "conf-local-yaml",
    srcs = glob(["conf/local/*.yaml"]),
)

filegroup(
    name = "conf-sandbox-yaml",
    srcs = glob(["conf/sandbox/*.yaml"]),
)

filegroup(
    name = "conf-aurora-yaml",
    srcs = glob(["conf/aurora/*"]),
)

filegroup(
    name = "conf-slurm-yaml",
    srcs = glob(["conf/slurm/*"]),
)

filegroup(
    name = "conf-yarn-yaml",
    srcs = glob(["conf/yarn/*"]),
)

filegroup(
    name = "conf-mesos-yaml",
    srcs = glob(["conf/mesos/*"]),
)

filegroup(
    name = "conf-marathon-yaml",
    srcs = glob(["conf/marathon/*"]),
)

filegroup(
    name = "conf-kubernetes-yaml",
    srcs = glob(["conf/kubernetes/*"]),
)

filegroup(
    name = "conf-local-heron-internals",
    srcs = ["conf/local/heron_internals.yaml"],
)

filegroup(
    name = "conf-local-metrics-sinks",
    srcs = ["conf/local/metrics_sinks.yaml"],
)

filegroup(
    name = "conf-local-client",
    srcs = ["conf/local/client.yaml"],
)

filegroup(
    name = "conf-local-packing",
    srcs = ["conf/local/packing.yaml"],
)

filegroup(
    name = "conf-local-scheduler",
    srcs = ["conf/local/scheduler.yaml"],
)

filegroup(
    name = "conf-local-statemgr",
    srcs = ["conf/local/statemgr.yaml"],
)

filegroup(
    name = "conf-local-uploader",
    srcs = ["conf/local/uploader.yaml"],
)
