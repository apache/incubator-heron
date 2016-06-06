#!/usr/bin/env python2.7
#
# Verifies required libraries and tools exist and are valid versions.
# Is so creates scripts/compile/env_exec.sh containing environment used
# by bazel when building.
#
# When changing this script, verify that it still works by running locally
# on a mac. Then verify the other environments by doing this:
#
#  cd docker
#  ./build-artifacts.sh ubuntu15.10 0.12.0 .
#  ./build-artifacts.sh ubuntu14.04 0.12.0 .
#  ./build-artifacts.sh centos7 0.12.0 .
#
import os
import re
import sys
import shutil
import getpass
import datetime
import platform
import subprocess

sys.path.append('3rdparty/python/semver')
import semver

######################################################################
# Architecture and system defines
######################################################################
ARCH_AND_SYS = {
  ('x86_64', 'Darwin') : ('IS_I386_MACOSX', 'IS_MACOSX'),
  ('x86_64', 'Linux' )  : ('IS_I386_LINUX',  'IS_LINUX'),
}

######################################################################
# Discover the name of the user compiling
######################################################################
def discover_user():
  return getpass.getuser()

######################################################################
# Discover the name of the host compiling
######################################################################
def discover_host():
  return platform.uname().node

######################################################################
# Get the time of the setup - does not change every time you compile
######################################################################
def discover_timestamp():
  return str(datetime.datetime.now())

######################################################################
# Get the processor the platform is running on
######################################################################
def discover_processor():
  return platform.uname().machine

######################################################################
# Get the operating system of the platform
######################################################################
def discover_os():
  return platform.uname().system

######################################################################
# Get the operating system version
######################################################################
def discover_os_version():
  return platform.uname().release

######################################################################
# Get the git sha of the branch - you are working
######################################################################
def discover_git_sha():
  return subprocess.check_output("git rev-parse HEAD", shell=True).strip("\n")

######################################################################
# Get the name of branch - you are working on
######################################################################
def discover_git_branch():
  return subprocess.check_output("git rev-parse --abbrev-ref HEAD", shell=True).strip("\n")

######################################################################
# Utility functions for system defines
######################################################################
def define_string(name, value):
  return '#define %s "%s"\n' % (name, value)

def define_value(name, value):
  return '#define %s %s\n' % (name, value)

######################################################################
# Discover where a program is located using the PATH variable
######################################################################
def which(program):
  def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

  def ext_candidates(fpath):
    yield fpath
    for ext in os.environ.get("PATHEXT", "").split(os.pathsep):
      yield fpath + ext

  fpath, fname = os.path.split(program)
  if fpath:
    if is_exe(program):
      return program
  else:
    for path in os.environ["PATH"].split(os.pathsep):
      exe_file = os.path.join(path, program)
      for candidate in ext_candidates(exe_file):
        if is_exe(candidate):
          return candidate

  return None

######################################################################
# Discover the real path of the program
######################################################################
def real_program_path(program_name):
  which_path = which(program_name)
  if which_path:
    return os.path.realpath(which_path)

  return None

######################################################################
# Get the real path of the program
######################################################################
def real_path(apath):
  return os.path.realpath(apath)

def fail(message):
  print("\nFAILED:  %s" % message)
  sys.exit(1)

# Assumes the version is at the end of the first line consisting of digits and dots
def get_trailing_version(line):
  version = re.search('([\d.]+)$', line)
  if version and '.' in version.group(0):
    return version.group(0)

def discover_version(path):
  if "python" in path:
    version_flag = "-V"
  else:
    version_flag = "--version"
  command = "%s %s" % (path, version_flag)
  version_output = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  first_line = version_output.split("\n")[0]
  version = get_trailing_version(first_line)
  if version:
    return version

  # on debian, /usr/bin/gcc --version returns this:
  #   gcc-5 (Debian 5.3.1-14) 5.3.1 20160409
  debian_line = re.search('.*?Debian.*?\s(\d[\d\.]+\d+)\s.*', first_line)
  if debian_line:
    version = get_trailing_version(debian_line.group(1))
    if version:
      return version

  # on centos, /usr/bin/gcc --version returns this:
  #   gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-4)
  redhat_line = re.search('(.*)\s+[0-9]+\s+\(Red Hat .*\)$', first_line)
  if redhat_line:
    version = get_trailing_version(redhat_line.group(1))
    if version:
      return version

  # on ubuntu, /usr/bin/gcc --version returns this:
  #   gcc-5 (Ubuntu 5.2.1-22ubuntu2) 5.2.1 20151010
  ubuntu_line = re.search('.*\s+\(Ubuntu .*\)\s+([\d\.]+)\s+\d+$', first_line)
  if ubuntu_line:
    version = get_trailing_version(ubuntu_line.group(1))
    if version:
      return version

  # on mac, /usr/bin/cpp --version returns this:
  #   Apple LLVM version 6.0 (clang-600.0.56) (based on LLVM 3.5svn)
  mac_line = re.search('^(Apple LLVM version\s+[\d\.]+)\s+\(clang.*', first_line)
  if mac_line:
    version = get_trailing_version(mac_line.group(1))
    if version:
      return version

  # on some centos versions, cmake --version returns this
  #   cmake version 2.6-patch 4
  centos_line = re.search('^cmake version\s+(\d.\d)-patch\s+(\d)', first_line)
  if centos_line:
    version = ".".join([centos_line.group(1), centos_line.group(2)])
    if version:
      return version

  # with python anaconda, --V returns this:
  # Python 2.7.11 :: Anaconda 2.2.0 (x86_64)
  anaconda_line = re.search('.*\s+Anaconda\s+.*\s', first_line)
  if anaconda_line:
    version = anaconda_line.group(0).split(' ')[1]
    if version:
      return version

  # python on debian, --V returns this:
  # Python 2.7.11+
  python_line = re.search('^Python\s+(\d[\d\.]+)\+{0,1}.*', first_line)
  if python_line:
    version = python_line.group(1)
    if version:
      return version


  fail ("Could not determine the version of %s from the following output\n%s\n%s" % (path, command, version_output))

def to_semver(version):
  # is version too short
  if re.search('^[\d]+\.[\d]+$', version):
    return "%s.0" % version

  # is version too long
  version_search = re.search('^([\d]+\.[\d]+\.[\d]+)\.[\d]+$', version)
  if version_search:
    return version_search.group(1)

  return version

def assert_min_version(path, min_version):
  version = discover_version(path)
  if not semver.match(to_semver(version), ">=%s" % to_semver(min_version)):
    fail("%s is version %s which is less than the required version %s" % (path, version, min_version))
  return version

######################################################################
# Discover the program using env variable/program name
######################################################################
def discover_program(program_name, env_variable = ""):
  env_value = program_name
  if env_variable:
    try:
      env_value = os.environ[env_variable]
    except KeyError:
      pass

  return real_program_path(env_value)

######################################################################
# Get the platform we are running
######################################################################
def discover_platform():
  output = subprocess.Popen(['uname'], stdout=subprocess.PIPE).communicate()
  return output[0].strip("\n")

######################################################################
# Make the file executable
######################################################################
def make_executable(path):
  mode = os.stat(path).st_mode
  mode |= (mode & 0444) >> 2    # copy R bits to X
  os.chmod(path, mode)

######################################################################
# Discover a tool needed to compile Heron
######################################################################
def discover_tool(program, msg, envvar, min_version = ''):
  VALUE = discover_program(program, envvar)
  if not VALUE:
    fail("""You need to have %s installed to build Heron.
Note: Some vendors install %s with a versioned name
(like /usr/bin/%s-4.8). You can set the %s environment
variable to specify the full path to yours.'""" % (program, program, program, envvar))

  print_value = VALUE
  if min_version:
    version = assert_min_version(VALUE, min_version)
    print_value = "%s (%s)" % (VALUE, version)

  print 'Using %s:\t%s' % (msg.ljust(20), print_value)
  return VALUE

######################################################################
# Discover the linker directory
######################################################################
def discover_linker(environ):
  BLDFLAG = '-B' + os.path.dirname(environ['LD'])
  return BLDFLAG

######################################################################
# Discover a tool needed but default to certain value if not able to
######################################################################
def discover_tool_default(program, msg, envvar, defvalue):
  VALUE = discover_program(program, envvar)
  if not VALUE:
    VALUE = defvalue
    print '%s:\tnot found, but ok' % (program.ljust(26))
  else:
    print 'Using %s:\t%s' % (msg.ljust(20), VALUE)
  return VALUE

######################################################################
# Discover the includes paths for files
######################################################################
def discover_include_paths(program):
  includes_command = program + ' -E -x c++ - -v 2>&1 < /dev/null '  \
    + '| sed -n \'/search starts here:/,/End of search list/p\' '  \
    + '| sed \'/#include.*/d\' ' \
    + '| sed \'/End of search list./d\' '
  includes = subprocess.Popen(includes_command, shell=True, stdout=subprocess.PIPE).communicate()[0]
  include_paths = [real_path(path.strip()) for path in includes.split('\n')]
  builtin_includes  = '\n'.join([
    '  cxx_builtin_include_directory: "%s"' % item for item in include_paths
  ])
  return builtin_includes

def export_env_to_file(out_file, env):
  if env in os.environ:
    out_file.write('export %s=%s\n' % (env, os.environ[env]))

######################################################################
# Generate the shell script that recreates the environment
######################################################################
def write_env_exec_file(platform, environ):

  env_exec_file = 'scripts/compile/env_exec.sh'
  out_file = open(env_exec_file, 'w')
  out_file.write('#!/bin/bash\n\n')
  out_file.write('set -eu \n\n')

  # If C environment is set, export them
  for env in ['CC', 'CPP', 'CFLAGS']:
    export_env_to_file(out_file, env)

  # If CXX environment is set, export them
  for env in ['CXX', 'CXXCPP', 'CXXFLAGS']:
    export_env_to_file(out_file, env)

  # If linker environment is set, export them
  for env in ['LDFLAGS', 'LIBS']:
    export_env_to_file(out_file, env)

  if 'CMAKE' in os.environ:
    out_file.write('export PATH=' + os.path.dirname(os.environ['CMAKE']) + ':$PATH\n')

  # Invoke the programs
  out_file.write('# Execute the input programs\n')
  out_file.write('$*')

  make_executable(env_exec_file)
  print 'Wrote the environment exec file %s' % (env_exec_file)


######################################################################
# Generate system defines based on processor, os and os version
######################################################################
def generate_system_defines():
  key = (discover_processor(), discover_os(), discover_os_version())
  if key in ARCH_AND_SYS:
    defines = ARCH_AND_SYS[key]
  else:
    key = (discover_processor(), discover_os())
    defines = ARCH_AND_SYS[key]

  strings = []
  for define in defines:
    strings.append(define_value(define, '1'))
  return "".join(strings)

######################################################################
# Write heron config header at config/heron-config.h
######################################################################
def write_heron_config_header(config_file):
  if os.path.exists(config_file): os.unlink(config_file)
  out_file = open(config_file, 'w')
  out_file.write(define_string('PACKAGE', 'heron'))
  out_file.write(define_string('PACKAGE_NAME', 'heron'))
  out_file.write(define_string('PACKAGE_VERSION', 'unversioned'))

  out_file.write(define_string('PACKAGE_COMPILE_USER', discover_user()))
  out_file.write(define_string('PACKAGE_COMPILE_HOST', discover_host()))
  out_file.write(define_string('PACKAGE_COMPILE_TIME', discover_timestamp()))

  out_file.write(define_string('GIT_SHA', discover_git_sha()))
  out_file.write(define_string('GIT_BRANCH', discover_git_branch()))
  out_file.write(generate_system_defines())
  out_file.close()
  print 'Wrote the heron config header file: \t"%s"' % (config_file)

######################################################################
# MAIN program that sets up your workspace for bazel
######################################################################
def main():
  env_map = dict()

  # Discover the platform
  platform = discover_platform()
  print "Platform %s" % platform

  # do differently on mac
  if platform == "Darwin":
    c_min = '4.2.1'
    cpp_min = '6.0' # on mac this will be clang version
  else:
    c_min = '4.8.1'
    cpp_min = c_min

  # Discover the tools environment
  env_map['CC'] = discover_tool('gcc','C compiler', 'CC', c_min)
  env_map['CXX'] = discover_tool('g++','C++ compiler', 'CXX', c_min)
  env_map['CPP'] = discover_tool('cpp','C preprocessor', 'CPP', cpp_min)
  env_map['CXXCPP'] = discover_tool('cpp','C++ preprocessor', 'CXXCPP', cpp_min)
  env_map['LD'] =  discover_tool('ld','linker', 'LD')
  env_map['BLDFLAG'] = discover_linker(env_map)

  # Discover the utilities
  env_map['AUTOMAKE'] = discover_tool('automake', 'Automake', 'AUTOMAKE', '1.9.6')
  env_map['AUTOCONF'] = discover_tool('autoconf', 'Autoconf', 'AUTOCONF', '2.6.3')
  env_map['MAKE'] = discover_tool('make', 'Make', 'MAKE', '3.81')
  env_map['CMAKE'] = discover_tool('cmake', 'CMake', 'CMAKE', '2.6.4')
  env_map['PYTHON2'] = discover_tool('python2.7', 'Python2', 'PYTHON2', '2.7')

  if platform == 'Darwin':
    env_map['AR'] = discover_tool('libtool', 'archiver', 'AR')
  else:
    env_map['AR'] = discover_tool('ar', 'archiver', 'AR')

  env_map['GCOV']= discover_tool('gcov','coverage tool', 'GCOV')
  env_map['DWP'] = discover_tool_default('dwp', 'dwp', 'DWP', '/usr/bin/dwp')
  env_map['NM'] = discover_tool_default('nm', 'nm', 'NM', '/usr/bin/nm')
  env_map['OBJCOPY'] = discover_tool_default('objcopy', 'objcopy', 'OBJCOPY', '/usr/bin/objcopy')
  env_map['OBJDUMP'] = discover_tool_default('objdump', 'objdump', 'OBJDUMP', '/usr/bin/objdump')
  env_map['STRIP'] = discover_tool_default('strip', "strip", 'STRIP', '/usr/bin/strip')

  # write the environment executable file
  write_env_exec_file(platform, env_map)

if __name__ == '__main__':
  main()
