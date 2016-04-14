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

def _impl(ctx):
  zip_input = ctx.label.name
  zip_output = ctx.outputs.zip
  src_list = []
  for src in ctx.files.srcs:
    src_list += [src.path]
  cmd = [
      "echo javadoc input: %s" % src_list,
      "mkdir %s" % zip_input,
      "javadoc -quiet -d %s %s" % (zip_input, " ".join(src_list)),
      "zip -q -r %s %s/*" % (zip_output.path, zip_input)]
  ctx.action(
      inputs = ctx.files.srcs,
      outputs = [zip_output],
      command = "\n".join(cmd))


javadoc = rule(
    attrs = {
        "srcs" : attr.label_list(allow_files = True)
    },
    implementation = _impl,
    outputs = {"zip" : "%{name}.zip"},
)
