licenses(["notice"])

package(default_visibility = ["//visibility:public"])

scala_binary(
    name = "streamlet-scala-examples-unshaded",
    srcs = glob(["org/apache/heron/examples/streamlet/scala/**/*.scala"]),
    main_class = "org.apache.heron.examples.streamlet.scala.ScalaIntegerProcessingTopology",
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/api/src/scala:api-scala",
        "//third_party/java:kryo",
        "@maven//:org_apache_commons_commons_lang3",
    ],
)

genrule(
    name = "heron-streamlet-scala-examples",
    srcs = [":streamlet-scala-examples-unshaded_deploy.jar"],
    outs = ["heron-streamlet-scala-examples.jar"],
    cmd = "cp $< $@",
)
