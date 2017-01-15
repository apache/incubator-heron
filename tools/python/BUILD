licenses(["notice"])

package(default_visibility = ["//visibility:public"])

action_listener(
    name = "compile_python",
    mnemonics = ["PexPython"],
    extra_actions = [":checkstyle_python"],
)

extra_action(
    name = "checkstyle_python",
    tools = [
        "//third_party/python/pylint",
        "//tools/java/src/com/twitter/bazel/checkstyle:checkstyle_python",
    ],
    requires_action_output = True,
    cmd = "$(location //tools/java/src/com/twitter/bazel/checkstyle:checkstyle_python) " +
          "--extra_action_file $(EXTRA_ACTION_FILE) " +
          "--pylint_file $$(pwd)/$(location //third_party/python/pylint)"
)