licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_toolchain(
    name = "heron_java_toolchain",
    bootclasspath = ["@bazel_tools//tools/jdk:bootclasspath"],
    encoding = "UTF-8",
    extclasspath = ["@bazel_tools//tools/jdk:extdir"],
    forcibly_disable_header_compilation = 0,
    genclass = ["@bazel_tools//tools/jdk:genclass"],
    header_compiler = ["@bazel_tools//tools/jdk:turbine"],
    ijar = ["@bazel_tools//tools/jdk:ijar"],
    javabuilder = ["@bazel_tools//tools/jdk:javabuilder"],
    javac = ["@bazel_tools//third_party/java/jdk/langtools:javac_jar"],
    jvm_opts = [
        "-Xbootclasspath/p:$(location @bazel_tools//third_party/java/jdk/langtools:javac_jar)",
    ],
    misc = [
#        "-XepDisableAllChecks",
#        "-extra_checks:on",
        "-Xlint:all",
        "-Werror",
    ],
    singlejar = ["@bazel_tools//tools/jdk:singlejar"],
    source_version = "8",
    target_version = "8",
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
