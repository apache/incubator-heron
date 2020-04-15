load("@rules_java//java:defs.bzl", "java_library")

common_deps_files = [
    "@maven//:com_google_guava_guava",
    "@maven//:commons_io_commons_io",
    "//third_party/java:powermock",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:utils-java",
    "//heron/scheduler-core/src/java:scheduler-java",
    "//third_party/java:junit4",
    "//third_party/java:mockito",
]

spi_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:statemgr-spi-java",
    "//heron/spi/src/java:uploader-spi-java",
    "//heron/spi/src/java:packing-spi-java",
    "//heron/spi/src/java:scheduler-spi-java",
    "//heron/spi/src/java:utils-spi-java",
]

scheduler_deps_files = \
    common_deps_files + \
    heron_java_proto_files() + \
    spi_deps_files

java_library(
    name = "scheduler-core-tests",
    srcs = glob(["**/*.java"]),
    deps = scheduler_deps_files + [
        "//heron/statemgrs/src/java:null-statemgr-java",
        "//heron/schedulers/src/java:null-scheduler-java",
        "//heron/packing/src/java:roundrobin-packing",
    ],
)

java_tests(
    size = "small",
    resources = ["//heron/scheduler-core/tests/resources:scheduler-resources"],
    test_classes = [
        "org.apache.heron.scheduler.LaunchRunnerTest",
        "org.apache.heron.scheduler.RuntimeManagerRunnerTest",
        "org.apache.heron.scheduler.RuntimeManagerMainTest",
        "org.apache.heron.scheduler.SubmitterMainTest",
        "org.apache.heron.scheduler.SchedulerMainTest",
        "org.apache.heron.scheduler.UpdateTopologyManagerTest",
        "org.apache.heron.scheduler.client.LibrarySchedulerClientTest",
        "org.apache.heron.scheduler.client.HttpServiceSchedulerClientTest",
        "org.apache.heron.scheduler.client.SchedulerClientFactoryTest",
        "org.apache.heron.scheduler.dryrun.SubmitDryRunRenderTest",
        "org.apache.heron.scheduler.dryrun.UpdateDryRunRenderTest",
        "org.apache.heron.scheduler.dryrun.JsonFormatterUtilsTest",
        "org.apache.heron.scheduler.server.SchedulerServerTest",
        "org.apache.heron.scheduler.utils.LauncherUtilsTest",
        "org.apache.heron.scheduler.utils.SchedulerUtilsTest",
    ],
    runtime_deps = [":scheduler-core-tests"],
)
