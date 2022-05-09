#!/usr/bin/env python3
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
""" Pex builder wrapper """

import pex.bin.pex as pexbin
from pex.resolve import requirement_options, resolver_options, target_options
from pex.resolve.requirement_configuration import RequirementConfiguration
from pex.common import die, safe_delete
from pex.tracer import TRACER
from pex.variables import ENV

import json
import os
import shutil
import sys


def dereference_symlinks(src):
    """
    Resolve all symbolic references that `src` points to.  Note that this
    is different than `os.path.realpath` as path components leading up to
    the final location may still be symbolic links.
    """
    while os.path.islink(src):
        src = os.path.join(os.path.dirname(src), os.readlink(src))

    return src


def parse_manifest(manifest_text):
    """Parse a json manifest.

    Manifest format:
      {
        "modules": [ {"src": "path_on_disk", "dest": "path_in_pex"}, ... ],
        "resources": [ {"src": "path_on_disk", "dest": "path_in_pex"}, ... ],
        "prebuiltLibraries": [ "path1", "path2", ... ],
        "requirements": [ "thing1", "thing2==version", ... ],
      }
    """
    return json.loads(manifest_text)


def main(args=None):
    args = args[:] if args else sys.argv[1:]
    args = [pexbin.transform_legacy_arg(arg) for arg in args]
    pparser = pexbin.configure_clp()

    try:
        separator = args.index("--")
        args, cmdline = args[:separator], args[separator + 1:]
    except ValueError:
        args, cmdline = args, []

    pparser.add_argument(
            "--manifest-file",
            dest="manifest_file",
            default=None,
            metavar="FILE",
            type=str,
            help="pex_wrapper manifest file.",
        )
    poptions = pparser.parse_args(args=args)

    manifest_file = poptions.manifest_file
    manifest_text = open(manifest_file, 'r').read()
    manifest = parse_manifest(manifest_text)

    reqs = manifest.get('requirements', [])
    requirement_configuration = RequirementConfiguration(requirements=reqs)
    try:
        resolver_configuration = resolver_options.configure(poptions)
    except resolver_options.InvalidConfigurationError as e:
        die(str(e))

    try:
        target_configuration = target_options.configure(poptions)
    except target_options.InterpreterNotFound as e:
        die(str(e))
    except target_options.InterpreterConstraintsNotSatisfied as e:
        die(str(e), exit_code=pexbin.CANNOT_SETUP_INTERPRETER)

    with ENV.patch(PEX_VERBOSE=str(poptions.verbosity),
                   PEX_ROOT=poptions.pex_root or ENV.PEX_ROOT):
        with TRACER.timed('Building pex'):
            pex_builder = pexbin.build_pex(
                requirement_configuration=requirement_configuration,
                resolver_configuration=resolver_configuration,
                target_configuration=target_configuration,
                options=poptions)

        # Add source files from the manifest
        for modmap in manifest.get('modules', []):
            src = modmap.get('src')
            dst = modmap.get('dest')

            # NOTE(agallagher): calls the `add_source` and `add_resource` below
            # hard-link the given source into the PEX temp dir.  Since OS X and
            # Linux behave different when hard-linking a source that is a
            # symbolic link (Linux does *not* follow symlinks), resolve any
            # layers of symlinks here to get consistent behavior.
            try:
                pex_builder.add_source(dereference_symlinks(src), dst)
            except OSError as err:
                # Maybe we just can't use hardlinks? Try again.
                if not pex_builder._copy:
                    pex_builder._copy = True
                    pex_builder.add_source(dereference_symlinks(src), dst)
                else:
                    raise RuntimeError(f"Failed to add {src}: {err}")

        # Add resources from the manifest
        for reqmap in manifest.get('resources', []):
            src = reqmap.get('src')
            dst = reqmap.get('dest')
            pex_builder.add_source(dereference_symlinks(src), dst)

        # Add eggs/wheels from the manifest
        for egg in manifest.get('prebuiltLibraries', []):
            try:
                pex_builder.add_dist_location(egg)
            except Exception as err:
                raise RuntimeError(f"Failed to add {egg}: {err}")

        # TODO(mikekap): Do something about manifest['nativeLibraries'].

        pexbin.log(f'Saving PEX file to {poptions.pex_name}',
                   V=poptions.verbosity)
        tmp_name = poptions.pex_name + '~'
        safe_delete(tmp_name)
        pex_builder.build(tmp_name)
        shutil.move(tmp_name, poptions.pex_name)


if __name__ == '__main__':
    main()
