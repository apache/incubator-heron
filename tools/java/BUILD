licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_toolchain(
    name = "heron_java_toolchain",
    encoding = "UTF-8",
    source_version = "7",
    target_version = "7",
    misc = [
        "-extra_checks:on",
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
        "//tools/java/src/com/twitter/bazel/checkstyle",
    ],
    requires_action_output = True,
    cmd = "$(location //tools/java/src/com/twitter/bazel/checkstyle) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--checkstyle_config_file tools/java/src/com/twitter/bazel/checkstyle/coding_style.xml"
)

