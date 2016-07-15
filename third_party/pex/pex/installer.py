# Copyright 2014 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import absolute_import, print_function

import os
import subprocess
import sys
import tempfile

from pkg_resources import Distribution, PathMetadata

from .common import safe_mkdtemp, safe_rmtree
from .compatibility import WINDOWS
from .interpreter import PythonInterpreter
from .tracer import TRACER
from .version import SETUPTOOLS_REQUIREMENT, WHEEL_REQUIREMENT

__all__ = (
  'Installer',
  'Packager'
)


def after_installation(function):
  def function_wrapper(self, *args, **kw):
    self._installed = self.run()
    if not self._installed:
      raise Installer.InstallFailure('Failed to install %s' % self._source_dir)
    return function(self, *args, **kw)
  return function_wrapper


class InstallerBase(object):
  SETUP_BOOTSTRAP_HEADER = "import sys"
  SETUP_BOOTSTRAP_MODULE = "sys.path.insert(0, %(path)r); import %(module)s"
  SETUP_BOOTSTRAP_FOOTER = """
__file__ = 'setup.py'
sys.argv[0] = 'setup.py'
exec(compile(open(__file__, 'rb').read(), __file__, 'exec'))
"""

  class Error(Exception): pass
  class InstallFailure(Error): pass
  class IncapableInterpreter(Error): pass

  def __init__(self, source_dir, strict=True, interpreter=None, install_dir=None):
    """
      Create an installer from an unpacked source distribution in source_dir.

      If strict=True, fail if any installation dependencies (e.g. distribute)
      are missing.
    """
    self._source_dir = source_dir
    self._install_tmp = install_dir or safe_mkdtemp()
    self._installed = None
    self._strict = strict
    self._interpreter = interpreter or PythonInterpreter.get()
    if not self._interpreter.satisfies(self.capability) and strict:
      raise self.IncapableInterpreter('Interpreter %s not capable of running %s' % (
          self._interpreter.binary, self.__class__.__name__))

  def mixins(self):
    """Return a map from import name to requirement to load into setup script prior to invocation.

       May be subclassed.
    """
    return {}

  @property
  def install_tmp(self):
    return self._install_tmp

  def _setup_command(self):
    """the setup command-line to run, to be implemented by subclasses."""
    raise NotImplementedError

  def _postprocess(self):
    """a post-processing function to run following setup.py invocation."""

  @property
  def capability(self):
    """returns the list of requirements for the interpreter to run this installer."""
    return list(self.mixins().values())

  @property
  def bootstrap_script(self):
    bootstrap_modules = []
    for module, requirement in self.mixins().items():
      path = self._interpreter.get_location(requirement)
      if not path:
        assert not self._strict  # This should be caught by validation
        continue
      bootstrap_modules.append(self.SETUP_BOOTSTRAP_MODULE % {'path': path, 'module': module})
    return '\n'.join(
        [self.SETUP_BOOTSTRAP_HEADER] + bootstrap_modules + [self.SETUP_BOOTSTRAP_FOOTER])

  def run(self):
    if self._installed is not None:
      return self._installed

    with TRACER.timed('Installing %s' % self._install_tmp, V=2):
      command = [self._interpreter.binary, '-']
      command.extend(self._setup_command())
      po = subprocess.Popen(command,
          stdin=subprocess.PIPE,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
          env=self._interpreter.sanitized_environment(),
          cwd=self._source_dir)
      so, se = po.communicate(self.bootstrap_script.encode('ascii'))
      self._installed = po.returncode == 0

    if not self._installed:
      name = os.path.basename(self._source_dir)
      print('**** Failed to install %s. stdout:\n%s' % (name, so.decode('utf-8')), file=sys.stderr)
      print('**** Failed to install %s. stderr:\n%s' % (name, se.decode('utf-8')), file=sys.stderr)
      return self._installed

    self._postprocess()
    return self._installed

  def cleanup(self):
    safe_rmtree(self._install_tmp)


class Installer(InstallerBase):
  """Install an unpacked distribution with a setup.py."""

  def __init__(self, source_dir, strict=True, interpreter=None):
    """
      Create an installer from an unpacked source distribution in source_dir.

      If strict=True, fail if any installation dependencies (e.g. setuptools)
      are missing.
    """
    super(Installer, self).__init__(source_dir, strict=strict, interpreter=interpreter)
    self._egg_info = None
    fd, self._install_record = tempfile.mkstemp()
    os.close(fd)

  def _setup_command(self):
    return ['install',
           '--root=%s' % self._install_tmp,
           '--prefix=',
           '--single-version-externally-managed',
           '--record', self._install_record]

  def _postprocess(self):
    installed_files = []
    egg_info = None
    with open(self._install_record) as fp:
      installed_files = fp.read().splitlines()
      for line in installed_files:
        if line.endswith('.egg-info'):
          assert line.startswith('/'), 'Expect .egg-info to be within install_tmp!'
          egg_info = line
          break

    if not egg_info:
      self._installed = False
      return self._installed

    installed_files = [os.path.relpath(fn, egg_info) for fn in installed_files if fn != egg_info]

    self._egg_info = os.path.join(self._install_tmp, egg_info[1:])
    with open(os.path.join(self._egg_info, 'installed-files.txt'), 'w') as fp:
      fp.write('\n'.join(installed_files))
      fp.write('\n')

    return self._installed

  @after_installation
  def egg_info(self):
    return self._egg_info

  @after_installation
  def root(self):
    egg_info = self.egg_info()
    assert egg_info
    return os.path.realpath(os.path.dirname(egg_info))

  @after_installation
  def distribution(self):
    base_dir = self.root()
    egg_info = self.egg_info()
    metadata = PathMetadata(base_dir, egg_info)
    return Distribution.from_location(base_dir, os.path.basename(egg_info), metadata=metadata)


class DistributionPackager(InstallerBase):
  def mixins(self):
    mixins = super(DistributionPackager, self).mixins().copy()
    mixins.update(setuptools=SETUPTOOLS_REQUIREMENT)
    return mixins

  def find_distribution(self):
    dists = os.listdir(self.install_tmp)
    if len(dists) == 0:
      raise self.InstallFailure('No distributions were produced!')
    elif len(dists) > 1:
      raise self.InstallFailure('Ambiguous source distributions found: %s' % (' '.join(dists)))
    else:
      return os.path.join(self.install_tmp, dists[0])


class Packager(DistributionPackager):
  """
    Create a source distribution from an unpacked setup.py-based project.
  """

  def _setup_command(self):
    if WINDOWS:
      return ['sdist', '--formats=zip', '--dist-dir=%s' % self._install_tmp]
    else:
      return ['sdist', '--formats=gztar', '--dist-dir=%s' % self._install_tmp]

  @after_installation
  def sdist(self):
    return self.find_distribution()


class EggInstaller(DistributionPackager):
  """
    Create a source distribution from an unpacked setup.py-based project.
  """

  def _setup_command(self):
    return ['bdist_egg', '--dist-dir=%s' % self._install_tmp]

  @after_installation
  def bdist(self):
    return self.find_distribution()


class WheelInstaller(DistributionPackager):
  """
    Create a source distribution from an unpacked setup.py-based project.
  """
  MIXINS = {
      'setuptools': SETUPTOOLS_REQUIREMENT,
      'wheel': WHEEL_REQUIREMENT,
  }

  def mixins(self):
    mixins = super(WheelInstaller, self).mixins().copy()
    mixins.update(self.MIXINS)
    return mixins

  def _setup_command(self):
    return ['bdist_wheel', '--dist-dir=%s' % self._install_tmp]

  @after_installation
  def bdist(self):
    return self.find_distribution()
