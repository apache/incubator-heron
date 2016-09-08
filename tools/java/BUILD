licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_toolchain(
    name = "heron_java_toolchain",
    encoding = "UTF-8",
    source_version = "7",
    target_version = "7",
    bootclasspath = ["@bazel_tools//tools/jdk:bootclasspath"],
    extclasspath = ["@bazel_tools//tools/jdk:extdir"],
    javac = ["@bazel_tools//third_party/java/jdk/langtools:javac_jar"],
    javabuilder = ["@bazel_tools//tools/jdk:JavaBuilder_deploy.jar"],
    singlejar = ["@bazel_tools//tools/jdk:SingleJar_deploy.jar"],
    genclass = ["@bazel_tools//tools/jdk:GenClass_deploy.jar"],
    ijar = ["@bazel_tools//tools/jdk:ijar"],
    misc = [
        "-extra_checks:on",
        "-Xlint:all",
        "-Werror",
    ],
)

action_listener(
    name = "compile_java",
    mnemonics = ["Javac"],
    extra_actions = [":checkstyle_java"],
)

extra_action(
    name = "checkstyle_java",
    tools = [
        "//tools/java/src/com/twitter/bazel/checkstyle:checkstyle_java",
    ],
    requires_action_output = True,
    cmd = "$(location //tools/java/src/com/twitter/bazel/checkstyle:checkstyle_java) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--heron_checkstyle_config_file tools/java/src/com/twitter/bazel/checkstyle/heron_coding_style.xml " +
          "--apache_checkstyle_config_file tools/java/src/com/twitter/bazel/checkstyle/apache_coding_style.xml"
)
