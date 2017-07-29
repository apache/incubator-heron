package(default_visibility = ["//visibility:public"])

api_deps_files = [
  "//heron/api/src/java:api-java",
  "//heron/common/src/java:basics-java",
]

apiserver_deps_files = \
  api_deps_files + [
    "//third_party/java:jetty-jersey-java",
    "//third_party/java:logging",
  ]

java_binary(
    name = 'heron-apiserver-unshaded',
    srcs = glob(["**/apiserver/**/*.java"]),
    main_class = "com.twitter.heron.apiserver.Runtime",
    resources = [
        "//heron/config/src/yaml:conf-yaml",
    ],
    deps = apiserver_deps_files)

genrule(
    name = "heron-apiserver",
    srcs = [":heron-apiserver-unshaded_deploy.jar"],
    outs = ["heron-apiserver.jar"],
    cmd  = "cp $< $@",
)
