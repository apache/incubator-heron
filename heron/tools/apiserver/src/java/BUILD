package(default_visibility = ["//visibility:public"])

api_deps_files = [
  "//heron/api/src/java:api-java",
  "//heron/common/src/java:basics-java",
  "//heron/storm/src/java:storm-compatibility-java",
]

api_server_deps_files = \
  api_deps_files + [
    "//third_party/java:jetty-jersey-java",
    "//third_party/java:logging",
  ]

java_library(
  name = "heron-api-server",
  srcs = glob(["**/server/**/*.java"]),
  deps = api_server_deps_files,  
)

java_binary(
    name = 'heron-api-server-unshaded',
    srcs = glob(["**/apiserver/**/*.java"]),
    main_class = "com.twitter.heron.apiserver.Runtime",
    deps = api_server_deps_files)

genrule(
    name = "api-server",
    srcs = [":heron-api-server-unshaded_deploy.jar"],
    outs = ["heron-api-server.jar"],
    cmd  = "cp $< $@",
)
