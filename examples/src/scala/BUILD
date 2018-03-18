licenses(["notice"])

package(default_visibility = ["//visibility:public"])

scala_binary(
    name = "streamlet-scala-examples-unshaded",
    srcs = glob(["com/twitter/heron/examples/streamlet/scala/**/*.scala"]),
    deps = [
            "//heron/api/src/java:api-java",
            "//heron/api/src/scala:api-scala",
        ],
    main_class = "com.twitter.heron.examples.streamlet.scala.ScalaIntegerProcessingTopology"
)

genrule(
    name = 'heron-streamlet-scala-examples',
    srcs = [":streamlet-scala-examples-unshaded_deploy.jar"],
    outs = ["heron-streamlet-scala-examples.jar"],
    cmd  = "cp $< $@",
)