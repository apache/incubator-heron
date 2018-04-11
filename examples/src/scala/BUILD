licenses(["notice"])

package(default_visibility = ["//visibility:public"])

scala_binary(
    name = "streamlet-scala-examples-unshaded",
    srcs = glob(["org/apache/heron/examples/streamlet/scala/**/*.scala"]),
    deps = [
        "@org_apache_commons_commons_lang3//jar",
        "//heron/api/src/java:api-java",
        "//heron/api/src/scala:api-scala",
        "//third_party/java:kryo"
    ],
    main_class = "org.apache.heron.examples.streamlet.scala.ScalaIntegerProcessingTopology"
)

genrule(
    name = 'heron-streamlet-scala-examples',
    srcs = [":streamlet-scala-examples-unshaded_deploy.jar"],
    outs = ["heron-streamlet-scala-examples.jar"],
    cmd  = "cp $< $@"
)