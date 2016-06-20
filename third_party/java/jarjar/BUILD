package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

java_binary(
    name = "jarjar_bin",
    main_class = "com.tonicsystems.jarjar.Main",
    runtime_deps = ["//third_party/java:jarjar"],
    resources = ["src/main/resources/com/tonicsystems/jarjar/help.txt"],
)
