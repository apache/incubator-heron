# Copyright 2015 The Bazel Authors. All rights reserved.
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
"""Self-extracting binary.

Generate a binary suitable for self-extraction:

self_extract_binary(
  name = "install.sh",
  launcher = "launcher.sh",
  resources = ["path1/file1", "path2/file2"],
  flatten_ressources = ["path3/file3"],
)

will generate a file 'install.sh' with a header (launcher.sh)
and a ZIP footer with the following entries:
  path1/
  path1/file1
  path2/
  path2/file2
  file3

"""

def _self_extract_binary(ctx):
  """Implementation for the self_extract_binary rule."""
  # This is a bit complex for stripping out timestamps
  zip_artifact = ctx.new_file(ctx.label.name + ".zip")
  create_dirs = [
      "mkdir -p ${tmpdir}/bin",
      "mkdir -p ${tmpdir}/conf",
      "mkdir -p ${tmpdir}/conf/local",
      "mkdir -p ${tmpdir}/dist",
      "mkdir -p ${tmpdir}/lib",
  ]
  cp_bin = [
      "cp %s ${tmpdir}/bin/%s" % (r.path, r.basename)
      for r in ctx.files.bin
  ] + [
      "chmod 0755 ${tmpdir}/bin/%s" % (r.basename)
      for r in ctx.files.bin
  ]
  cp_conf = [
      "cp %s ${tmpdir}/conf/%s" % (r.path, r.basename)
      for r in ctx.files.conf
  ] + [
      "chmod 0644 ${tmpdir}/conf/%s" % (r.basename)
      for r in ctx.files.conf
  ]
  cp_local = [
      "cp %s ${tmpdir}/conf/local/%s" % (r.path, r.basename)
      for r in ctx.files.local
  ] + [
      "chmod 0644 ${tmpdir}/conf/local/%s" % (r.basename)
      for r in ctx.files.local
  ]
  cp_dist = [
      "cp %s ${tmpdir}/dist/%s" % (r.path, r.basename)
      for r in ctx.files.dist
  ] + [
      "chmod 0644 ${tmpdir}/dist/%s" % (r.basename)
      for r in ctx.files.dist
  ]
  cp_lib = [
      "cp %s ${tmpdir}/lib/%s" % (r.path, r.basename)
      for r in ctx.files.lib
  ] + [
      "chmod 0644 ${tmpdir}/lib/%s" % (r.basename)
      for r in ctx.files.lib
  ]
  ctx.action(
      inputs = ctx.files.bin + ctx.files.conf + ctx.files.local + ctx.files.dist + ctx.files.lib,
      outputs = [zip_artifact],
      command = "\n".join([
          "tmpdir=$(mktemp -d ${TMPDIR:-/tmp}/tmp.XXXXXXXX)",
          "trap \"rm -fr ${tmpdir}\" EXIT"
          ] + create_dirs + cp_bin + cp_conf + cp_local + cp_dist + cp_lib + [
              "find ${tmpdir} -exec touch -t 198001010000.00 '{}' ';'",
              "(d=${PWD}; cd ${tmpdir}; zip -rq ${d}/%s *)" % zip_artifact.path,
              ]),
      mnemonic = "ZipBin",
  )
  ctx.action(
      inputs = [ctx.file.launcher, zip_artifact],
      outputs = [ctx.outputs.executable],
      command = "\n".join([
          "cat %s %s > %s" % (ctx.file.launcher.path,
                              zip_artifact.path,
                              ctx.outputs.executable.path),
          "zip -qA %s" % ctx.outputs.executable.path
      ]),
      mnemonic = "BuildSelfExtractable",
  )

self_extract_binary = rule(
    _self_extract_binary,
    executable = True,
    attrs = {
        "launcher": attr.label(
            mandatory=True,
            allow_files=True,
            single_file=True),
        "bin": attr.label_list(
            default=[],
            allow_files=True),
        "conf": attr.label_list(
            default=[],
            allow_files=True),
        "local": attr.label_list(
            default=[],
            allow_files=True),
        "dist": attr.label_list(
            default=[],
            allow_files=True),
        "lib": attr.label_list(
            default=[],
            allow_files=True),
        },
    )
