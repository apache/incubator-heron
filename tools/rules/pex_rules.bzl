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

pex_file_types = FileType([".py"])
egg_file_types = FileType([".egg", ".whl"])
pex_test_file_types = FileType(["_unittest.py", "_test.py"])

def collect_transitive_sources(ctx):
  source_files = set(order="compile")
  for dep in ctx.attr.deps:
    source_files += dep.transitive_py_files

  source_files += pex_file_types.filter(ctx.files.srcs)
  return source_files

def collect_transitive_eggs(ctx):
  transitive_eggs = set(order="compile")
  for dep in ctx.attr.deps:
    transitive_eggs += dep.transitive_egg_files
  transitive_eggs += egg_file_types.filter(ctx.files.eggs)
  return transitive_eggs

def collect_transitive_reqs(ctx):
  transitive_reqs = set(order="compile")
  for dep in ctx.attr.deps:
    transitive_reqs += dep.transitive_reqs
  transitive_reqs += ctx.attr.reqs

  return transitive_reqs

def pex_library_impl(ctx):
  transitive_sources = collect_transitive_sources(ctx)
  transitive_eggs = collect_transitive_eggs(ctx)
  transitive_reqs = collect_transitive_reqs(ctx)
  return struct(
      files = set(),
      transitive_py_files = transitive_sources,
      transitive_reqs = transitive_reqs,
      transitive_egg_files = transitive_eggs)

def get_package_from_path(pkg_path):
  return pkg_path.replace('/', '.')[:-3]

# Converts map to text format. Each file on separate line.
def textify_pex_input(input_map):
  kv_pairs = ['\t%s:%s' % (pkg, input_map[pkg]) for pkg in input_map.keys()]
  return '\n'.join(kv_pairs)

def write_pex_manifest_text(modules, prebuilt_libs, resources, requirements):
  return ('modules:\n%s\nrequirements:\n%s\nresources:\n%s\nnativeLibraries:\nprebuiltLibraries:\n%s' % (
      textify_pex_input(modules),
      textify_pex_input(dict(zip(requirements,requirements))),
      textify_pex_input(resources),
      textify_pex_input(prebuilt_libs)))

def get_module_path(ctx, pkg_path):
  genfiles_dir = ctx.configuration.genfiles_dir.path
  if pkg_path.startswith(genfiles_dir):
    module_path = pkg_path[len(genfiles_dir):]
    if module_path.startswith('/'):
      return module_path[1:]
    return module_path 
  return pkg_path

def make_manifest(ctx, output):
  transitive_sources = collect_transitive_sources(ctx)
  transitive_reqs = collect_transitive_reqs(ctx)
  transitive_eggs = collect_transitive_eggs(ctx)
  transitive_resources = ctx.files.resources
  pex_modules = {}
  pex_prebuilt_libs = {}
  pex_resources = {}
  pex_requirements = []
  for f in transitive_sources:
    pex_modules[get_module_path(ctx, f.path)] = f.path

  for f in transitive_eggs:
    pex_prebuilt_libs[get_module_path(ctx, f.path)] = f.path

  for f in transitive_resources:
    pex_resources[get_module_path(ctx, f.path)] = f.path

  manifest_text = write_pex_manifest_text(pex_modules, pex_prebuilt_libs, pex_resources, transitive_reqs)
  ctx.action(
    inputs = list(transitive_sources) + list(transitive_eggs) + list(transitive_resources),
    outputs = [ output ],
    command = ('touch %s && echo "%s" > %s' % (output.path, manifest_text, output.path)))

def common_pex_arguments(entry_point, deploy_pex_path, manifest_file_path):
  arguments = ['--disable-cache']
  arguments += ['--entry-point', entry_point]

  # Our internal build environment requires extra args injected and this is a brutal hack. Ideally
  # bazel would provide a mechanism to swap in env-specific global params here
  #EXTRA_PEX_ARGS#
  arguments += [deploy_pex_path]
  arguments += [manifest_file_path]
  return arguments

def pex_binary_impl(ctx):
  if not ctx.file.main:
    main_file = pex_file_types.filter(ctx.files.srcs)[0]
  else:
    main_file = ctx.file.main

  # Package name is same as folder name followed by filename (without .py extension)
  main_pkg = get_package_from_path(main_file.path)

  deploy_pex = ctx.new_file(
      ctx.configuration.bin_dir, ctx.outputs.executable, '.pex')

  manifest_file = ctx.new_file(
      ctx.configuration.bin_dir, deploy_pex, '.manifest')
  make_manifest(ctx, manifest_file)

  transitive_sources = collect_transitive_sources(ctx)
  transitive_eggs = collect_transitive_eggs(ctx)
  transitive_resources = ctx.files.resources
  pexbuilder = ctx.executable._pexbuilder

  # form the arguments to pex builder
  arguments = [] if ctx.attr.zip_safe else ["--not-zip-safe"]
  arguments += common_pex_arguments(main_pkg, deploy_pex.path, manifest_file.path)

  # form the inputs to pex builder
  inputs =  [main_file, manifest_file]
  inputs += list(transitive_sources)
  inputs += list(transitive_eggs)
  inputs += list(transitive_resources)

  ctx.action(
      mnemonic = "PexPython",
      inputs = inputs,
      outputs = [deploy_pex],
      executable = pexbuilder,
      arguments = arguments)

  executable = ctx.outputs.executable
  ctx.action(
      inputs = [deploy_pex],
      outputs = [executable],
      command = "cp %s %s" % (deploy_pex.path, executable.path))

  return struct(files = set([executable]))

def pex_test_impl(ctx):
  deploy_pex = ctx.new_file(
      ctx.configuration.bin_dir, ctx.outputs.executable, '.pex')

  manifest_file = ctx.new_file(
      ctx.configuration.bin_dir, deploy_pex, '.manifest')
  make_manifest(ctx, manifest_file)

  # Get pex test files
  transitive_sources = collect_transitive_sources(ctx)
  transitive_eggs = collect_transitive_eggs(ctx)
  transitive_resources = ctx.files.resources
  pexbuilder = ctx.executable._pexbuilder
  pex_test_files = pex_test_file_types.filter(pex_file_types.filter(transitive_sources))
  test_run_args = ' '.join([f.path for f in pex_test_files])

  arguments = common_pex_arguments('pytest', deploy_pex.path, manifest_file.path)

  ctx.action(
      inputs = [manifest_file] + list(transitive_sources) + list(transitive_eggs) + list(transitive_resources),
      executable = pexbuilder,
      outputs = [ deploy_pex ],
      mnemonic = "PexPython",
      arguments = arguments)

  executable = ctx.outputs.executable
  ctx.action(
      inputs = [ deploy_pex ],
      outputs = [ executable ],
      command = "echo PYTHONDONTWRITEBYTECODE=1 \"`pwd`/%s `pwd`/%s\" > %s" % (deploy_pex.path, test_run_args, executable.path))

  return struct(files = set([executable]))


pex_srcs_attr = attr.label_list(
    flags=["DIRECT_COMPILE_TIME_INPUT"],
    allow_files = pex_file_types
)

pex_deps_attr = attr.label_list(
    providers = ["transitive_py_files", "transitive_egg_files", "transitive_reqs"],
    allow_files = False)

eggs_attr = attr.label_list(
    flags=["DIRECT_COMPILE_TIME_INPUT"],
    allow_files = egg_file_types)

resource_attr = attr.label_list(
    allow_files = True)

pex_attrs = {
    "srcs": pex_srcs_attr,
    "deps": pex_deps_attr,
    "eggs": eggs_attr,
    "reqs": attr.string_list(),
    "resources": resource_attr,
    "main": attr.label(allow_files=True, single_file=True)
}

pex_bin_attrs = pex_attrs + {
    "zip_safe": attr.bool(
        default = True,
        mandatory = False
    )
}

pex_library = rule(
    pex_library_impl,
    attrs = pex_attrs
)

pex_binary_outputs = {
    "deploy_pex": "%{name}.pex"
}

pex_binary = rule(
    pex_binary_impl,
    executable = True,
    attrs = pex_bin_attrs + {
        "_pexbuilder": attr.label(
            default = Label("//third_party/pex:_pex"),
            allow_files = False,
            executable = True,
        ),
    },
)

pex_test = rule(
    pex_test_impl,
    executable = True,
    attrs = pex_attrs + {
        "_pexbuilder": attr.label(
            default = Label("//third_party/pex:_pex"),
            allow_files = False,
            executable = True,
        ),
    },
    test = True,
)
