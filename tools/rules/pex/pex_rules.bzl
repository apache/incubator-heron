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
# https://github.com/twitter/heron/blob/master/tools/rules/pex_rules.bzl

"""Python pex rules for Bazel

[![Build Status](https://travis-ci.org/benley/bazel_rules_pex.svg?branch=master)](https://travis-ci.org/benley/bazel_rules_pex)

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

pex_file_types = FileType([".py"])
egg_file_types = FileType([".egg", ".whl"])
req_file_types = FileType([".txt"])

# Repos file types according to: https://www.python.org/dev/peps/pep-0527/
repo_file_types = FileType([
    ".egg",
    ".whl",
    ".tar.gz",
    ".zip",
    ".tar",
    ".tar.bz2",
    ".tar.xz",
    ".tar.Z",
    ".tgz",
    ".tbz"
])

# As much as I think this test file naming convention is a good thing, it's
# probably a bad idea to impose it as a policy to all OSS users of these rules,
# so I guess let's skip it.
#
# pex_test_file_types = FileType(["_unittest.py", "_test.py"])


def _collect_transitive_sources(ctx):
  source_files = depset(order="postorder")
  for dep in ctx.attr.deps:
    source_files += dep.py.transitive_sources
  source_files += pex_file_types.filter(ctx.files.srcs)
  return source_files


def _collect_transitive_eggs(ctx):
  transitive_eggs = depset(order="postorder")
  for dep in ctx.attr.deps:
    if hasattr(dep.py, "transitive_eggs"):
      transitive_eggs += dep.py.transitive_eggs
  transitive_eggs += egg_file_types.filter(ctx.files.eggs)
  return transitive_eggs


def _collect_transitive_reqs(ctx):
  transitive_reqs = depset(order="postorder")
  for dep in ctx.attr.deps:
    if hasattr(dep.py, "transitive_reqs"):
      transitive_reqs += dep.py.transitive_reqs
  transitive_reqs += ctx.attr.reqs
  return transitive_reqs


def _collect_repos(ctx):
  repos = {}
  for dep in ctx.attr.deps:
    if hasattr(dep.py, "repos"):
      repos += dep.py.repos
  for file in repo_file_types.filter(ctx.files.repos):
    repos.update({file.dirname : True})
  return repos.keys()


def _collect_transitive(ctx):
  return struct(
      # These rules don't use transitive_sources internally; it's just here for
      # parity with the native py_library rule type.
      transitive_sources = _collect_transitive_sources(ctx),
      transitive_eggs = _collect_transitive_eggs(ctx),
      transitive_reqs = _collect_transitive_reqs(ctx),
      # uses_shared_libraries = ... # native py_library has this. What is it?
  )


def _pex_library_impl(ctx):
  transitive_files = depset(ctx.files.srcs)
  for dep in ctx.attr.deps:
    transitive_files += dep.default_runfiles.files
  return struct(
      files = depset(),
      py = _collect_transitive(ctx),
      runfiles = ctx.runfiles(
          collect_default = True,
          transitive_files = depset(transitive_files),
      )
  )


def _gen_manifest(py, runfiles):
  """Generate a manifest for pex_wrapper.

  Returns:
      struct(
          modules = [struct(src = "path_on_disk", dest = "path_in_pex"), ...],
          requirements = ["pypi_package", ...],
          prebuiltLibraries = ["path_on_disk", ...],
      )
  """

  pex_files = []

  for f in runfiles.files:
    dpath = f.short_path
    if dpath.startswith("../"):
      dpath = dpath[3:]
    pex_files.append(
        struct(
            src = f.path,
            dest = dpath,
        ),
    )

  return struct(
      modules = pex_files,
      requirements = list(py.transitive_reqs),
      prebuiltLibraries = [f.path for f in py.transitive_eggs],
  )


def _pex_binary_impl(ctx):
  transitive_files = depset(ctx.files.srcs)

  if ctx.attr.entrypoint and ctx.file.main:
    fail("Please specify either entrypoint or main, not both.")
  if ctx.attr.entrypoint:
    main_file = None
    main_pkg = ctx.attr.entrypoint
  elif ctx.file.main:
    main_file = ctx.file.main
  else:
    main_file = pex_file_types.filter(ctx.files.srcs)[0]
  if main_file:
    # Translate main_file's short path into a python module name
    main_pkg = main_file.short_path.replace('/', '.')[:-3]
    transitive_files += [main_file]

  deploy_pex = ctx.new_file(
      ctx.configuration.bin_dir, ctx.outputs.executable, '.pex')

  py = _collect_transitive(ctx)
  repos = _collect_repos(ctx)

  for dep in ctx.attr.deps:
    transitive_files += dep.default_runfiles.files
  runfiles = ctx.runfiles(
      collect_default = True,
      transitive_files = transitive_files,
  )

  manifest_file = ctx.new_file(
      ctx.configuration.bin_dir, deploy_pex, '_manifest')

  manifest = _gen_manifest(py, runfiles)

  ctx.file_action(
      output = manifest_file,
      content = manifest.to_json(),
  )

  pexbuilder = ctx.executable._pexbuilder

  # form the arguments to pex builder
  arguments =  [] if ctx.attr.zip_safe else ["--not-zip-safe"]
  arguments += [] if ctx.attr.pex_use_wheels else ["--no-use-wheel"]
  if ctx.attr.no_index:
    arguments += ["--no-index"]
  if ctx.attr.disable_cache:
    arguments += ["--disable-cache"]
  for req_file in ctx.files.req_files:
    arguments += ["--requirement", req_file.path]
  for repo in repos:
    arguments += ["--repo", repo]
  for egg in py.transitive_eggs:
    arguments += ["--find-links", egg.dirname]
  arguments += [
      "--pex-root", ".pex",  # May be redundant since we also set PEX_ROOT
      "--entry-point", main_pkg,
      "--output-file", deploy_pex.path,
      "--cache-dir", ".pex/build",
      manifest_file.path,
  ]
  if ctx.attr.interpreter != "":
    arguments += ["--python-shebang", ctx.attr.interpreter]

  # form the inputs to pex builder
  _inputs = (
      [manifest_file] +
      list(runfiles.files) +
      list(py.transitive_eggs)
  )

  ctx.action(
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
          'PATH': '/bin:/usr/bin:/usr/local/bin',
          'PEX_VERBOSE': str(ctx.attr.pex_verbosity),
          'PEX_PYTHON': str(ctx.attr.interpreter),
          'PEX_ROOT': '.pex',  # So pex doesn't try to unpack into $HOME/.pex
      },
      arguments = arguments,
  )

  executable = ctx.outputs.executable

  # There isn't much point in having both foo.pex and foo as identical pex
  # files, but someone is probably relying on that behaviour by now so we might
  # as well keep doing it.
  ctx.action(
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
  ctx.template_action(
      template = ctx.file.launcher_template,
      output = output_file,
      substitutions = {
          "%test_runner%": _get_runfile_path(ctx, test_runner),
          "%test_files%": " \\\n    ".join(test_file_paths),
      },
      executable = True,
  )

  transitive_files = depset(ctx.files.srcs + [test_runner])
  for dep in ctx.attr.deps:
    transitive_files += dep.default_runfiles

  return struct(
      runfiles = ctx.runfiles(
          files = [output_file],
          transitive_files = transitive_files,
          collect_default = True
      )
  )


pex_attrs = {
    "srcs": attr.label_list(flags = ["DIRECT_COMPILE_TIME_INPUT"],
                            allow_files = pex_file_types),
    "deps": attr.label_list(allow_files = False,
                            providers = ["py"]),
    "eggs": attr.label_list(flags = ["DIRECT_COMPILE_TIME_INPUT"],
                            allow_files = egg_file_types),
    "reqs": attr.string_list(),
    "req_files": attr.label_list(flags = ["DIRECT_COMPILE_TIME_INPUT"],
                            allow_files = req_file_types),
    "no_index": attr.bool(default=False),
    "disable_cache": attr.bool(default=False),
    "repos": attr.label_list(flags = ["DIRECT_COMPILE_TIME_INPUT"],
                            allow_files = repo_file_types),
    "data": attr.label_list(allow_files = True,
                            cfg = "data"),

    # required for pex_library targets in third_party subdirs
    # but theoretically a common attribute for all rules
    "licenses": attr.license(),

    # Used by pex_binary and pex_*test, not pex_library:
    "_pexbuilder": attr.label(
        default = Label("//pex:pex_wrapper"),
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
    "main": attr.label(allow_files = True,
                       single_file = True),
    "entrypoint": attr.string(),
    "interpreter": attr.string(),
    "pex_use_wheels": attr.bool(default=True),
    "pex_verbosity": attr.int(default=0),
    "zip_safe": attr.bool(
        default = True,
        mandatory = False,
    ),
})

pex_library = rule(
    _pex_library_impl,
    attrs = pex_attrs
)

pex_binary_outputs = {
    "deploy_pex": "%{name}.pex"
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

  req_files: Add requirements from the given requirements files. Must be provided as labels.

    This feature will reduce build determinism!  It tells pex to resolve all
    the transitive python dependencies and fetch them from pypi.

    It is recommended that you use `eggs` or specify `no_index` instead where possible.

  no_index: If True, don't use pypi to resolve dependencies for `reqs` and `req_files`; Default: False

  disable_cache: Disable caching in the pex tool entirely. Default: False

  repos: Additional repository labels (filegroups of wheel/egg files) to look for requirements.

  data: Files to include as resources in the final pex binary.

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
            cfg = "data",
        ),
        "launcher_template": attr.label(
            allow_files = True,
            single_file = True,
            default = Label("//pex:testlauncher.sh.template"),
        ),
    }),
)


def pex_pytest(name, srcs, deps=[], eggs=[], data=[],
               args=[],
               flaky=False,
               licenses=[],
               local=None,
               size=None,
               timeout=None,
               tags=[],
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
          "@setuptools_whl//file",
      ],
      entrypoint = "pytest",
      licenses = licenses,
      testonly = True,
      **kwargs
  )
  _pytest_pex_test(
      name = name,
      runner = ":%s_runner" % name,
      args = args,
      data = data,
      flaky = flaky,
      licenses = licenses,
      local = local,
      size = size,
      srcs = srcs,
      timeout = timeout,
      tags = tags,
  )


def pex_repositories():
  """Rules to be invoked from WORKSPACE for remote dependencies."""
  native.http_file(
      name = 'pytest_whl',
      url = 'https://pypi.python.org/packages/8c/7d/f5d71f0e28af32388e07bd4ce0dbd2b3539693aadcae4403266173ec87fa/pytest-3.2.3-py2.py3-none-any.whl',
      sha256 = '81a25f36a97da3313e1125fce9e7bbbba565bc7fec3c5beb14c262ddab238ac1'
  )

  native.http_file(
      name = 'py_whl',
      url = 'https://pypi.python.org/packages/53/67/9620edf7803ab867b175e4fd23c7b8bd8eba11cb761514dcd2e726ef07da/py-1.4.34-py2.py3-none-any.whl',
      sha256 = '2ccb79b01769d99115aa600d7eed99f524bf752bba8f041dc1c184853514655a'
  )

  native.http_file(
      name = "wheel_src",
      url = "https://pypi.python.org/packages/c9/1d/bd19e691fd4cfe908c76c429fe6e4436c9e83583c4414b54f6c85471954a/wheel-0.29.0.tar.gz",
      sha256 = "1ebb8ad7e26b448e9caa4773d2357849bf80ff9e313964bcaf79cbf0201a1648",
  )

  native.http_file(
      name = "setuptools_whl",
      url = "https://pypi.python.org/packages/e5/53/92a8ac9d252ec170d9197dcf988f07e02305a06078d7e83a41ba4e3ed65b/setuptools-33.1.1-py2.py3-none-any.whl",
      sha256 = "4ed8f634b11fbba8c0ba9db01a8d24ad464f97a615889e9780fc74ddec956711",
  )

  native.http_file(
      name = "pex_src",
      url = "https://pypi.python.org/packages/58/ab/ac60cf7f2e855a6e621f2bbfe88c4e2479658650c2af5f1f26f9fc6deefb/pex-1.2.13.tar.gz",
      sha256 = "53b592ec04dc2829d8ea3a13842bfb378e1531ae788b10d0d5a1ea6cac45388c",
  )

  native.http_file(
      name = "requests_src",
      url = "https://pypi.python.org/packages/b0/e1/eab4fc3752e3d240468a8c0b284607899d2fbfb236a56b7377a329aa8d09/requests-2.18.4.tar.gz",
      sha256 = "9c443e7324ba5b85070c4a818ade28bfabedf16ea10206da1132edaa6dda237e",
  )

  native.http_file(
      name = "urllib3_whl",
      url = "https://pypi.python.org/packages/63/cb/6965947c13a94236f6d4b8223e21beb4d576dc72e8130bd7880f600839b8/urllib3-1.22-py2.py3-none-any.whl",
      sha256 = "06330f386d6e4b195fbfc736b297f58c5a892e4440e54d294d7004e3a9bbea1b",
  )

  native.http_file(
      name = "idna_whl",
      url = "https://pypi.python.org/packages/27/cc/6dd9a3869f15c2edfab863b992838277279ce92663d334df9ecf5106f5c6/idna-2.6-py2.py3-none-any.whl",
      sha256 = "8c7309c718f94b3a625cb648ace320157ad16ff131ae0af362c9f21b80ef6ec4",
  )

  native.http_file(
      name = "certifi_whl",
      url = "https://pypi.python.org/packages/40/66/06130724e8205fc8c105db7edb92871c7fff7d31324d7f4405c762624a43/certifi-2017.7.27.1-py2.py3-none-any.whl",
      sha256 = "54a07c09c586b0e4c619f02a5e94e36619da8e2b053e20f594348c0611803704",
  )

  native.http_file(
      name = "chardet_whl",
      url = "https://pypi.python.org/packages/bc/a9/01ffebfb562e4274b6487b4bb1ddec7ca55ec7510b22e4c51f14098443b8/chardet-3.0.4-py2.py3-none-any.whl",
      sha256 = "fc323ffcaeaed0e0a02bf4d117757b98aed530d9ed4531e3e15460124c106691",
  )

  native.new_http_archive(
      name = "virtualenv",
      url = "https://pypi.python.org/packages/d4/0c/9840c08189e030873387a73b90ada981885010dd9aea134d6de30cd24cb8/virtualenv-15.1.0.tar.gz",
      sha256 = "02f8102c2436bb03b3ee6dede1919d1dac8a427541652e5ec95171ec8adbc93a",
      strip_prefix = "virtualenv-15.1.0",
      build_file_content = "\n".join([
          "py_binary(",
          "    name = 'virtualenv',",
          "    srcs = ['virtualenv.py'],",
          # exclude .pyc: Otherwise bazel detects a change after running virtualenv.py
          "    data = glob(['**/*'], exclude=['*.pyc']),",
          "    visibility = ['//visibility:public'],",
          ")",
      ])
  )
