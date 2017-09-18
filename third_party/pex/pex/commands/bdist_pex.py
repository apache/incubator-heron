import os
import shlex
from distutils import log

from setuptools import Command

from pex.bin.pex import build_pex, configure_clp, make_relative_to_root
from pex.common import die
from pex.compatibility import ConfigParser, StringIO, string, to_unicode
from pex.variables import ENV


# Suppress checkstyle violations due to setuptools command requirements.
class bdist_pex(Command):  # noqa
  description = "create a PEX file from a source distribution"  # noqa

  user_options = [  # noqa
      ('bdist-all', None, 'pexify all defined entry points'),
      ('bdist-dir=', None, 'the directory into which pexes will be written, default: dist.'),
      ('pex-args=', None, 'additional arguments to the pex tool'),
  ]

  boolean_options = [  # noqa
    'bdist-all',
  ]

  def initialize_options(self):
    self.bdist_all = False
    self.bdist_dir = None
    self.pex_args = ''

  def finalize_options(self):
    self.pex_args = shlex.split(self.pex_args)

  def _write(self, pex_builder, target, script=None):
    builder = pex_builder.clone()

    if script is not None:
      builder.set_script(script)

    builder.build(target)

  def parse_entry_points(self):
    def split_and_strip(entry_point):
      console_script, entry_point = entry_point.split('=', 2)
      return console_script.strip(), entry_point.strip()

    raw_entry_points = self.distribution.entry_points

    if isinstance(raw_entry_points, string):
      parser = ConfigParser()
      parser.readfp(StringIO(to_unicode(raw_entry_points)))
      if parser.has_section('console_scripts'):
        return dict(parser.items('console_scripts'))
    elif isinstance(raw_entry_points, dict):
      try:
        return dict(split_and_strip(script)
            for script in raw_entry_points.get('console_scripts', []))
      except ValueError:
        pass
    elif raw_entry_points is not None:
      die('When entry_points is provided, it must be a string or dict.')

    return {}

  def run(self):
    name = self.distribution.get_name()
    version = self.distribution.get_version()
    parser, options_builder = configure_clp()
    package_dir = os.path.dirname(os.path.realpath(os.path.expanduser(
        self.distribution.script_name)))

    if self.bdist_dir is None:
      self.bdist_dir = os.path.join(package_dir, 'dist')

    options, reqs = parser.parse_args(self.pex_args)

    # Update cache_dir with pex_root in case this is being called directly.
    if options.cache_dir:
      options.cache_dir = make_relative_to_root(options.cache_dir)
    options.interpreter_cache_dir = make_relative_to_root(options.interpreter_cache_dir)

    if options.entry_point or options.script:
      die('Must not specify entry_point or script to --pex-args')

    reqs = [package_dir] + reqs

    with ENV.patch(PEX_VERBOSE=str(options.verbosity), PEX_ROOT=options.pex_root):
      pex_builder = build_pex(reqs, options, options_builder)

    console_scripts = self.parse_entry_points()

    target = os.path.join(self.bdist_dir, name + '-' + version + '.pex')
    if self.bdist_all:
      # Write all entry points into unversioned pex files.
      for script_name in console_scripts:
        target = os.path.join(self.bdist_dir, script_name)
        log.info('Writing %s to %s' % (script_name, target))
        self._write(pex_builder, target, script=script_name)
    elif name in console_scripts:
      # The package has a namesake entry point, so use it.
      log.info('Writing %s to %s' % (name, target))
      self._write(pex_builder, target, script=name)
    else:
      # The package has no namesake entry point, so build an environment pex.
      log.info('Writing environment pex into %s' % target)
      self._write(pex_builder, target, script=None)
