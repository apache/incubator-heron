# Copyright (C) 2016 The Android Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Javadoc rule.

def _impl(ctx):
    zip_output = ctx.outputs.zip
    source_jars = depset(transitive = [depset(j[JavaInfo].source_jars) for j in ctx.attr.libs])
    transitive_jar_set = depset(transitive = [j[JavaInfo].transitive_deps for j in ctx.attr.libs])
    transitive_jar_paths = [j.path for j in transitive_jar_set.to_list()]
    dir = ctx.outputs.zip.path + ".dir"
    source = ctx.outputs.zip.path + ".source"
    external_docs = ["http://docs.oracle.com/javase/8/docs/api"] + ctx.attr.external_docs
    cmd = [
        "rm -rf %s" % source,
        "mkdir %s" % source,
        " && ".join(["unzip -qud %s %s" % (source, j.path) for j in source_jars.to_list()]),
        "rm -rf %s" % dir,
        "mkdir %s" % dir,
        " ".join([
            ctx.file._javadoc.path,
            "-Xdoclint:-missing",
            "-protected",
            "-encoding UTF-8",
            "-charset UTF-8",
            "-notimestamp",
            "-quiet",
            "-windowtitle '%s'" % ctx.attr.title,
            " ".join(["-link %s" % url for url in external_docs]),
            "-sourcepath %s" % source,
            "-subpackages ",
            ":".join(ctx.attr.pkgs),
            " -classpath ",
            ":".join(transitive_jar_paths),
            "-d %s" % dir,
        ]),
        "find %s -exec touch -t 198001010000 '{}' ';'" % dir,
        "(cd %s && zip -qr ../%s *)" % (dir, ctx.outputs.zip.basename),
    ]
    ctx.actions.run_shell(
        inputs = list(transitive_jar_set.to_list()) + list(source_jars.to_list()) + ctx.files._jdk,
        outputs = [zip_output],
        command = " && ".join(cmd),
    )

java_doc = rule(
    attrs = {
        "libs": attr.label_list(allow_files = False),
        "pkgs": attr.string_list(),
        "title": attr.string(),
        "external_docs": attr.string_list(),
        "_javadoc": attr.label(
            default = Label("@local_jdk//:bin/javadoc"),
            allow_single_file = True,
        ),
        "_jdk": attr.label(
            default = Label("@local_jdk//:bin/javadoc"),
            allow_files = True,
        ),
    },
    outputs = {"zip": "%{name}.zip"},
    implementation = _impl,
)
