# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Originally derived from:
# https://github.com/apache/incubator-heron/blob/master/tools/rules/pex_rules.bzl

"""Python pex rules for Bazel

### Setup

Add something like this to your WORKSPACE file:

    git_repository(
        name = "io_bazel_rules_pex",
        remote = "https://github.com/benley/bazel_rules_pex.git",
        tag = "0.3.0",
    )
    load("@io_bazel_rules_pex//pex:pex_rules.bzl", "pex_repositories")
    pex_repositories()

In a BUILD file where you want to use these rules, or in your
`tools/build_rules/prelude_bazel` file if you want them present repo-wide, add:

    load(
        "@io_bazel_rules_pex//pex:pex_rules.bzl",
        "pex_binary",
        "pex_library",
        "pex_test",
        "pex_pytest",
    )

Lastly, make sure that `tools/build_rules/BUILD` exists, even if it is empty,
so that Bazel can find your `prelude_bazel` file.
"""

pex_file_types = [".py"]
egg_file_types = [".egg", ".whl"]

PexProviderInfo = provider(fields = ["transitive_sources", "transitive_eggs", "transitive_reqs"])

def _collect_transitive_sources(ctx):
    return depset(
        ctx.files.srcs,
        transitive = [dep[PexProviderInfo].transitive_sources for dep in ctx.attr.deps],
    )

def _collect_transitive_eggs(ctx):
    return depset(
        ctx.files.eggs,
        transitive = [dep[PexProviderInfo].transitive_eggs for dep in ctx.attr.deps],
    )

def _collect_transitive_reqs(ctx):
    return depset(
        ctx.attr.reqs,
        transitive = [dep[PexProviderInfo].transitive_reqs for dep in ctx.attr.deps],
    )

def _collect_transitive(ctx):
    return PexProviderInfo(
        # These rules don't use transitive_sources internally; it's just here for
        # parity with the native py_library rule type.
        transitive_sources = _collect_transitive_sources(ctx),
        transitive_eggs = _collect_transitive_eggs(ctx),
        transitive_reqs = _collect_transitive_reqs(ctx),
        # uses_shared_libraries = ... # native py_library has this. What is it?
    )

def _pex_library_impl(ctx):
    transitive_files = depset(
        ctx.files.srcs,
        transitive = [dep.default_runfiles.files for dep in ctx.attr.deps],
    )
    return struct(
        providers = [_collect_transitive(ctx)],
        runfiles = ctx.runfiles(
            collect_default = True,
            transitive_files = transitive_files,
        ),
    )

def _gen_manifest(py, runfiles, resources):
    """Generate a manifest for pex_wrapper.

    Returns:
        struct(
            modules = [struct(src = "path_on_disk", dest = "path_in_pex"), ...],
            requirements = ["pypi_package", ...],
            prebuiltLibraries = ["path_on_disk", ...],
            resources = ["path_on_disk", ...],
        )
    """

    pex_files = []

    for f in runfiles.files.to_list():
        dpath = f.short_path
        if dpath.startswith("../"):
            dpath = dpath[3:]
        pex_files.append(
            struct(
                src = f.path,
                dest = dpath,
            ),
        )

    res_files = []

    for f in resources:
        dpath = f.short_path
        if dpath.startswith("../"):
            dpath = dpath[3:]
        res_files.append(
            struct(
                src = f.path,
                dest = dpath,
            ),
        )

    return struct(
        modules = pex_files,
        requirements = py.transitive_reqs.to_list(),
        prebuiltLibraries = [f.path for f in py.transitive_eggs.to_list()],
        resources = res_files,
    )

def _pex_binary_impl(ctx):
    if ctx.attr.entrypoint and ctx.file.main:
        fail("Please specify either entrypoint or main, not both.")
    if ctx.attr.entrypoint:
        main_file = None
        main_pkg = ctx.attr.entrypoint
    elif ctx.file.main:
        main_file = ctx.file.main
    else:
        main_file = ctx.files.srcs[0]

    transitive_files = list(ctx.files.srcs)
    if main_file:
        # Translate main_file's short path into a python module name
        main_pkg = main_file.short_path.replace("/", ".")[:-3]
        transitive_files.append(main_file)

    deploy_pex = ctx.actions.declare_file("%s.pex" % ctx.attr.name)

    py = _collect_transitive(ctx)

    transitive_files = depset(
        transitive_files,
        transitive = [dep.default_runfiles.files for dep in ctx.attr.deps],
    )

    runfiles = ctx.runfiles(
        collect_default = True,
        transitive_files = depset(transitive_files.to_list()),
    )

    resources = ctx.files.resources
    manifest_file = ctx.actions.declare_file("%s.pex_manifest" % ctx.attr.name)

    manifest = _gen_manifest(py, runfiles, resources)

    ctx.actions.write(
        output = manifest_file,
        content = manifest.to_json(),
    )

    pexbuilder = ctx.executable._pexbuilder

    # form the arguments to pex builder
    arguments = [] if ctx.attr.zip_safe else ["--not-zip-safe"]
    arguments += [] if ctx.attr.pex_use_wheels else ["--no-use-wheel"]
    if ctx.attr.interpreter:
        arguments += ["--python", ctx.attr.interpreter]
    for platform in ctx.attr.platforms:
        arguments += ["--platform", platform]
    for egg in py.transitive_eggs.to_list():
        arguments += ["--find-links", egg.dirname]
    arguments += [
        "--pex-root",
        ".pex",  # May be redundant since we also set PEX_ROOT
        "--entry-point",
        main_pkg,
        "--output-file",
        deploy_pex.path,
        "--disable-cache",
        manifest_file.path,
    ]
    #EXTRA_PEX_ARGS#

    # form the inputs to pex builder
    _inputs = (
        [manifest_file] +
        runfiles.files.to_list() +
        py.transitive_eggs.to_list() +
        list(resources)
    )

    ctx.actions.run(
        mnemonic = "PexPython",
        inputs = _inputs,
        outputs = [deploy_pex],
        executable = pexbuilder,
        execution_requirements = {
            "requires-network": "1",
        },
        env = {
            # TODO(benley): Write a repository rule to pick up certain
            # PEX-related environment variables (like PEX_VERBOSE) from the
            # system.
            # Also, what if python is actually in /opt or something?
            "PATH": "/bin:/usr/bin:/usr/local/bin",
            "PEX_VERBOSE": str(ctx.attr.pex_verbosity),
            "PEX_ROOT": ".pex",  # So pex doesn't try to unpack into $HOME/.pex
        },
        arguments = arguments,
    )

    executable = ctx.outputs.executable

    # There isn't much point in having both foo.pex and foo as identical pex
    # files, but someone is probably relying on that behaviour by now so we might
    # as well keep doing it.
    ctx.actions.run_shell(
        mnemonic = "LinkPex",
        inputs = [deploy_pex],
        outputs = [executable],
        command = "ln -f {pex} {exe} 2>/dev/null || cp -f {pex} {exe}".format(
            pex = deploy_pex.path,
            exe = executable.path,
        ),
    )

    return struct(
        files = depset([executable]),  # Which files show up in cmdline output
        runfiles = runfiles,
    )

def _get_runfile_path(ctx, f):
    """Return the path to f, relative to runfiles."""
    if ctx.workspace_name:
        return ctx.workspace_name + "/" + f.short_path
    else:
        return f.short_path

def _pex_pytest_impl(ctx):
    test_runner = ctx.executable.runner
    output_file = ctx.outputs.executable

    test_file_paths = ["${RUNFILES}/" + _get_runfile_path(ctx, f) for f in ctx.files.srcs]
    ctx.actions.expand_template(
        template = ctx.file.launcher_template,
        output = output_file,
        substitutions = {
            "%test_runner%": _get_runfile_path(ctx, test_runner),
            "%test_files%": " \\\n    ".join(test_file_paths),
        },
        is_executable = True,
    )

    transitive_files = depset(ctx.files.srcs + [test_runner])
    for dep in ctx.attr.deps:
        transitive_files += dep.default_runfiles

    return struct(
        runfiles = ctx.runfiles(
            files = [output_file],
            transitive_files = transitive_files,
            collect_default = True,
        ),
    )

pex_attrs = {
    "srcs": attr.label_list(
        flags = ["DIRECT_COMPILE_TIME_INPUT"],
        allow_files = pex_file_types,
    ),
    "deps": attr.label_list(
        allow_files = False,
        providers = [PexProviderInfo],
    ),
    "eggs": attr.label_list(
        flags = ["DIRECT_COMPILE_TIME_INPUT"],
        allow_files = egg_file_types,
    ),
    "reqs": attr.string_list(),
    "data": attr.label_list(allow_files = True),

    # Used by pex_binary and pex_*test, not pex_library:
    "_pexbuilder": attr.label(
        default = Label("//tools/rules/pex:pex_wrapper"),
        executable = True,
        cfg = "host",
    ),
}

def _dmerge(a, b):
    """Merge two dictionaries, a+b

    Workaround for https://github.com/bazelbuild/skydoc/issues/10
    """
    return dict(a.items() + b.items())

pex_bin_attrs = _dmerge(pex_attrs, {
    "main": attr.label(allow_single_file = True),
    "entrypoint": attr.string(),
    "interpreter": attr.string(),
    "platforms": attr.string_list(),
    "pex_use_wheels": attr.bool(default = True),
    "pex_verbosity": attr.int(default = 0),
    "resources": attr.label_list(allow_files = True),
    "zip_safe": attr.bool(
        default = True,
        mandatory = False,
    ),
})

pex_library = rule(
    _pex_library_impl,
    attrs = pex_attrs,
)

pex_binary_outputs = {
    "deploy_pex": "%{name}.pex",
}

pex_binary = rule(
    _pex_binary_impl,
    executable = True,
    attrs = pex_bin_attrs,
    outputs = pex_binary_outputs,
)
"""Build a deployable pex executable.

Args:
  deps: Python module dependencies.

    `pex_library` and `py_library` rules should work here.

  eggs: `.egg` and `.whl` files to include as python packages.

  reqs: External requirements to retrieve from pypi, in `requirements.txt` format.

    This feature will reduce build determinism!  It tells pex to resolve all
    the transitive python dependencies and fetch them from pypi.

    It is recommended that you use `eggs` instead where possible.

  data: Files to include as resources in the final pex binary.

    Putting other rules here will cause the *outputs* of those rules to be
    embedded in this one. Files will be included as-is. Paths in the archive
    will be relative to the workspace root.

  resources: Similar to data, typically used for web resources.
    Putting other rules here will cause the *outputs* of those rules to be
    embedded in this one. Files will be included as-is. Paths in the archive
    will be relative to the workspace root.

  main: File to use as the entrypoint.

    If unspecified, the first file from the `srcs` attribute will be used.

  entrypoint: Name of a python module to use as the entrypoint.

    e.g. `your.project.main`

    If unspecified, the `main` attribute will be used.
    It is an error to specify both main and entrypoint.

  interpreter: Path to the python interpreter the pex should to use in its shebang line.
"""

pex_test = rule(
    _pex_binary_impl,
    executable = True,
    attrs = pex_bin_attrs,
    outputs = pex_binary_outputs,
    test = True,
)

_pytest_pex_test = rule(
    _pex_pytest_impl,
    executable = True,
    test = True,
    attrs = _dmerge(pex_attrs, {
        "runner": attr.label(
            executable = True,
            mandatory = True,
            cfg = "target",
        ),
        "launcher_template": attr.label(
            allow_single_file = True,
            default = Label("//tools/rules/pex:testlauncher.sh.template"),
        ),
    }),
)

def pex_pytest(
        name,
        srcs,
        deps = [],
        eggs = [],
        data = [],
        args = [],
        flaky = False,
        local = None,
        size = None,
        timeout = None,
        tags = [],
        **kwargs):
    """A variant of pex_test that uses py.test to run one or more sets of tests.

    This produces two things:

      1. A pex_binary (`<name>_runner`) containing all your code and its
         dependencies, plus py.test, and the entrypoint set to the py.test
         runner.
      2. A small shell script to launch the `<name>_runner` executable with each
         of the `srcs` enumerated as commandline arguments.  This is the actual
         test entrypoint for bazel.

    Almost all of the attributes that can be used with pex_test work identically
    here, including those not specifically mentioned in this docstring.
    Exceptions are `main` and `entrypoint`, which cannot be used with this macro.

    Args:

      srcs: List of files containing tests that should be run.
    """
    if "main" in kwargs:
        fail("Specifying a `main` file makes no sense for pex_pytest.")
    if "entrypoint" in kwargs:
        fail("Do not specify `entrypoint` for pex_pytest.")

    pex_binary(
        name = "%s_runner" % name,
        srcs = srcs,
        deps = deps,
        data = data,
        eggs = eggs + [
            "@pytest_whl//file",
            "@py_whl//file",
        ],
        entrypoint = "pytest",
        testonly = True,
        **kwargs
    )
    _pytest_pex_test(
        name = name,
        runner = ":%s_runner" % name,
        args = args,
        data = data,
        flaky = flaky,
        local = local,
        size = size,
        srcs = srcs,
        timeout = timeout,
        tags = tags,
    )
