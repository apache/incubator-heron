licenses(["notice"])

package(default_visibility = ["//visibility:public"])

action_listener(
    name = "compile_python",
    extra_actions = [":checkstyle_python"],
    mnemonics = ["PexPython"],
)

extra_action(
    name = "checkstyle_python",
    cmd = "$(execpath //tools/java/src/org/apache/bazel/checkstyle:checkstyle_python) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--pylint_file $$(pwd)/$(execpath //third_party/python/pylint)",
    requires_action_output = True,
    tools = [
        "//third_party/python/pylint",
        "//tools/java/src/org/apache/bazel/checkstyle:checkstyle_python",
    ],
)
