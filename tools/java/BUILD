licenses(["notice"])

package(default_visibility = ["//visibility:public"])

action_listener(
    name = "compile_java",
    extra_actions = [":checkstyle_java"],
    mnemonics = ["Javac"],
)

extra_action(
    name = "checkstyle_java",
    cmd = "$(execpath //tools/java/src/org/apache/bazel/checkstyle:checkstyle_java) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--heron_checkstyle_config_file tools/java/src/org/apache/bazel/checkstyle/heron_coding_style.xml " +
          "--apache_checkstyle_config_file tools/java/src/org/apache/bazel/checkstyle/apache_coding_style.xml",
    requires_action_output = True,
    tools = [
        "//tools/java/src/org/apache/bazel/checkstyle:checkstyle_java",
    ],
)
