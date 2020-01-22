licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "test-data-files",
    srcs = glob(["**/*.json"]),
)

scala_binary(
    name = "scala-integration-tests-unshaded",
    srcs = glob(["org/apache/heron/integration_test/**/*.scala"]),
    main_class = "org.apache.heron.integration_test.topology.scala_streamlet_with_filter_and_transform.ScalaStreamletWithFilterAndTransform",
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/scala:api-scala",
        "//integration_test/src/java:common",
        "//integration_test/src/java:core",
    ],
)

genrule(
    name = "scala-integration-tests",
    srcs = [":scala-integration-tests-unshaded_deploy.jar"],
    outs = ["scala-integration-tests.jar"],
    cmd = "cp $< $@",
)
