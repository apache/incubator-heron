#!/usr/bin/env python2.7

from __future__ import print_function
import os
import sys
import functools
import shutil
import tempfile
import optparse
import zipfile


# Try to detect if we're running from source via the repo.  Add appropriate
# deps to our python path, so we can find the twitter libs and
# setuptools at runtime.  Also, locate the `pkg_resources` modules
# via our local setuptools import.
if not zipfile.is_zipfile(sys.argv[0]):
    sys.modules.pop('twitter', None)
    sys.modules.pop('twitter.common', None)
    sys.modules.pop('twitter.common.python', None)

    root = os.path.join(
        os.sep.join(__file__.split(os.sep)[:-6]), 'pex/_pex.runfiles/__main__/third_party')
    sys.path.insert(0, os.path.join(root, 'pex'))
    sys.path.insert(0, os.path.join(root, 'setuptools'))
    setuptools_py =  os.path.join(root, 'setuptools')
    pkg_resources_py = os.path.join(root, 'setuptools/pkg_resources.py')

# Otherwise, we're running from a PEX, so import the `pkg_resources`
# module via a resource.
else:
    with open(os.path.join(tempfile.mkdtemp(dir="/tmp"), "abc.txt"), "w") as foo:
        foo.write(sys.argv[0])
        foo.write(zipfile.is_zipfile(sys.argv[0]))
    import pkg_resources
    pkg_resources_py_tmp = tempfile.NamedTemporaryFile(
        prefix='pkg_resources.py')
    pkg_resources_py_tmp.write(
        pkg_resources.resource_string(__name__, 'pkg_resources.py'))
    pkg_resources_py_tmp.flush()
    pkg_resources_py = pkg_resources_py_tmp.name

from pex.bin.pex import build_pex, configure_clp, resolve_interpreter, CANNOT_SETUP_INTERPRETER
from pex.common import die
from pex.interpreter import PythonInterpreter
from pex.version import SETUPTOOLS_REQUIREMENT, WHEEL_REQUIREMENT


def dereference_symlinks(src):
    """
    Resolve all symbolic references that `src` points to.  Note that this
    is different than `os.path.realpath` as path components leading up to
    the final location may still be symbolic links.
    """
    while os.path.islink(src):
        src = os.path.join(os.path.dirname(src), os.readlink(src))

    return src

# The format is first line will say if it is modules/resources/nativeLibraries/prebuiltLibraries
# Then it will follow with a tab indentation for key:value of corresponding item.
def parse_manifest(manifest_text):
    lines = manifest_text.split('\n')
    manifest = {}
    curr_key = ''
    for line in lines:
       tokens = line.split(':')
       if len(tokens) != 2:
           continue
       elif not line.startswith('\t'):
           manifest[tokens[0]] = {}
           curr_key = tokens[0]
       else:
           # line is of form <tab>key:value
           manifest[curr_key][tokens[0][1:]] = tokens[1]
    return manifest

def resolve_or_die(interpreter, requirement, options):
    resolve = functools.partial(resolve_interpreter, options.interpreter_cache_dir, options.repos)

    interpreter = resolve(interpreter, requirement)
    if interpreter is None:
      die('Could not find compatible interpreter that meets requirement %s' % requirement, CANNOT_SETUP_INTERPRETER)
    return interpreter

def main():
    # These are the options that this class will accept from the rule
    parser = optparse.OptionParser(usage="usage: %prog [options] output")
    parser.add_option('--entry-point', default='__main__')
    parser.add_option('--no-pypi', action='store_false', dest='pypi', default=True)
    parser.add_option('--disable-cache', action='store_true', dest='disable_cache', default=False)
    parser.add_option('--not-zip-safe', action='store_false', dest='zip_safe', default=True)
    parser.add_option('--python', default="/usr/bin/python2.7")
    parser.add_option('--find-links', dest='find_links', default='')
    options, args = parser.parse_args()

    if len(args) == 2:
        output = args[0]
        manifest_text = open(args[1], 'r').read()
    elif len(args) == 1:
        output = args[0]
        manifest_text = sys.stdin.read()
    else:
        parser.error("'output' positional argument is required")
        return 1

    if manifest_text.startswith('"') and  manifest_text.endswith('"'):
        manifest_text = manifest_text[1:len(manifest_text) - 1]

    # The manifest is passed via stdin, as it can sometimes get too large
    # to be passed as a CLA.
    with open(os.path.join(tempfile.mkdtemp(dir="/tmp"), "stderr"), "w") as x:
        x.write(manifest_text)
    manifest = parse_manifest(manifest_text)

    # Setup a temp dir that the PEX builder will use as its scratch dir.
    tmp_dir = tempfile.mkdtemp()
    try:
        # These are the options that pex will use
        pparser, resolver_options_builder = configure_clp()

        # Disabling wheels since the PyYAML wheel is incompatible with the Travis CI linux host.
        # Enabling Trace logging in pex/tracer.py shows this upon failure:
        #  pex: Target package WheelPackage('file:///tmp/tmpR_gDlG/PyYAML-3.11-cp27-cp27mu-linux_x86_64.whl')
        #  is not compatible with CPython-2.7.3 / linux-x86_64
        poptions, preqs = pparser.parse_args(['--no-use-wheel'] + sys.argv)
        poptions.entry_point = options.entry_point
        poptions.find_links = options.find_links
        poptions.pypi = options.pypi
        poptions.python = options.python
        poptions.zip_safe = options.zip_safe
        poptions.disable_cache = options.disable_cache


        #print("pex options: %s" % poptions)
        os.environ["PATH"] = ".:%s:/bin:/usr/bin" % poptions.python

        # The version of pkg_resources.py (from setuptools) on some distros is too old for PEX. So
        # we keep a recent version in and force it into the process by constructing a custom
        # PythonInterpreter instance using it.
        interpreter = PythonInterpreter(
            poptions.python,
            PythonInterpreter.from_binary(options.python).identity,
            extras={
                # TODO: Fix this to resolve automatically
                ('setuptools', '18.0.1'): 'third_party/pex/setuptools-18.0.1-py2.py3-none-any.whl',
                ('wheel', '0.23.0'): 'third_party/pex/wheel-0.23.0-py2.7.egg'
            })

        # resolve setuptools
        interpreter = resolve_or_die(interpreter, SETUPTOOLS_REQUIREMENT, poptions)

        # possibly resolve wheel
        if interpreter and poptions.use_wheel:
          interpreter = resolve_or_die(interpreter, WHEEL_REQUIREMENT, poptions)

        # Add prebuilt libraries listed in the manifest.
        reqs = manifest.get('requirements', {}).keys()
        #if len(reqs) > 0:
        #  print("pex requirements: %s" % reqs)
        pex_builder = build_pex(reqs, poptions,
                                resolver_options_builder, interpreter=interpreter)

        # Set whether this PEX as zip-safe, meaning everything will stayed zipped up
        # and we'll rely on python's zip-import mechanism to load modules from
        # the PEX.  This may not work in some situations (e.g. native
        # libraries, libraries that want to find resources via the FS).
        pex_builder.info.zip_safe = options.zip_safe

        # Set the starting point for this PEX.
        pex_builder.info.entry_point = options.entry_point

        pex_builder.add_source(
            dereference_symlinks(pkg_resources_py),
            os.path.join(pex_builder.BOOTSTRAP_DIR, 'pkg_resources.py'))

        # Add the sources listed in the manifest.
        for dst, src in manifest['modules'].iteritems():
            # NOTE(agallagher): calls the `add_source` and `add_resource` below
            # hard-link the given source into the PEX temp dir.  Since OS X and
            # Linux behave different when hard-linking a source that is a
            # symbolic link (Linux does *not* follow symlinks), resolve any
            # layers of symlinks here to get consistent behavior.
            try:
                pex_builder.add_source(dereference_symlinks(src), dst)
            except OSError as e:
                raise Exception("Failed to add {}: {}".format(src, e))

        # Add resources listed in the manifest.
        for dst, src in manifest['resources'].iteritems():
            # NOTE(agallagher): see rationale above.
            pex_builder.add_resource(dereference_symlinks(src), dst)

        # Add prebuilt libraries listed in the manifest.
        for req in manifest.get('prebuiltLibraries', []):
            try:
                pex_builder.add_dist_location(req)
            except Exception as e:
                raise Exception("Failed to add {}: {}".format(req, e))

        # TODO(mikekap): Do something about manifest['nativeLibraries'].

        # Generate the PEX file.
        pex_builder.build(output)

    # Always try cleaning up the scratch dir, ignoring failures.
    finally:
        shutil.rmtree(tmp_dir, True)


sys.exit(main())

