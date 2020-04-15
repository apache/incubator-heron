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

def jarjar_binary_impl(ctx):
    src_file = ctx.file.src
    shade_file = ctx.file.shade
    jarjar = ctx.executable._jarjar
    class_jar = ctx.outputs.class_jar

    ctx.actions.run(
        executable = jarjar,
        inputs = [src_file, shade_file],
        outputs = [class_jar],
        arguments = ["process", shade_file.path, src_file.path, class_jar.path],
    )

    return [
        JavaInfo(
            output_jar = class_jar,
            compile_jar = class_jar,
        ),
        DefaultInfo(files = depset([class_jar])),
    ]

jarjar_attrs = {
    "src": attr.label(
        allow_single_file = [".jar"],
    ),
    "shade": attr.label(
        allow_single_file = True,
    ),
    "deps": attr.label_list(),
    "_jarjar": attr.label(
        default = Label("//third_party/java/jarjar:jarjar_bin"),
        allow_files = True,
        executable = True,
        cfg = "host",
    ),
}

jarjar_binary = rule(
    jarjar_binary_impl,
    attrs = jarjar_attrs,
    outputs = {
        "class_jar": "%{name}.jar",
    },
    provides = [JavaInfo],
)
