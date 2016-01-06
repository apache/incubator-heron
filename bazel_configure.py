#!/usr/bin/env python2.7 
import os, sys
import subprocess, shutil

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
  return subprocess.check_output("whoami", shell=True).strip("\n")
  
######################################################################
# Discover the name of the host compiling
######################################################################
def discover_host():
  return subprocess.check_output("uname -n", shell=True).strip("\n")
  
######################################################################
# Get the time of the setup - does not change every time you compile
######################################################################
def discover_timestamp():
  return subprocess.check_output("date", shell=True).strip("\n")
  
######################################################################
# Get the processor the platform is running on
######################################################################
def discover_processor():
  return subprocess.check_output("uname -m", shell=True).strip("\n")

######################################################################
# Get the operating system of the platform
######################################################################
def discover_os():
  return subprocess.check_output("uname -s", shell=True).strip("\n")

######################################################################
# Get the operating system version
######################################################################
def discover_os_version():
  return subprocess.check_output("uname -r", shell=True).strip("\n")
  
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

######################################################################
# Discover the program using env variable/program name 
######################################################################
def discover_program(program_name, env_variable = ""):
  env_value = program_name
  if len(env_variable) > 0:
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
def discover_tool(program, msg, envvar = ""):
  VALUE = discover_program(program, envvar)
  if not VALUE:
    print 'You need to have %s installed to build Heron.' % (program)
    print 'Note: Some vendors install %s with a versioned name' % (program)
    print '(like /usr/bin/%s-4.8). You can set the %s environment' % (program, envvar)
    print 'variable to specify the full path to yours.'
    sys.exit(1)
  print 'Using %s: \t\t "%s"' % (msg, VALUE)
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
    print '%s : \t\t\t not found, but ok' % (program)
  else:
    print 'Using %s : \t\t\t "%s"' % (msg, VALUE)
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

######################################################################
# Generate the shell script that recreates the environment
######################################################################
def write_env_exec_file(platform, environ):

  env_exec_file = 'scripts/env_exec.sh'
  out_file = open(env_exec_file, 'w')
  out_file.write('#!/bin/bash\n\n')
  out_file.write('set -eu \n\n')

  # If C environment is set, export them
  if 'CC' in os.environ:
    out_file.write('export CC=' + os.environ['CC'] + '\n')
  if 'CPP' in os.environ:
    out_file.write('export CPP=' + os.environ['CPP'] + '\n')
  if 'CFLAGS' in os.environ:
    out_file.write('export CFLAGS=' + os.environ['CFLAGS'] + '\n')
  if 'CPPFLAGS' in os.environ:
    out_file.write('export CPPFLAGS=' + os.environ['CPPFLAGS'] + '\n')

  # If CXX environment is set, export them
  if 'CXX' in os.environ:
    out_file.write('export CXX=' + os.environ['CXX'] + '\n')
  if 'CXXCPP' in os.environ:
    out_file.write('export CXXCPP=' + os.environ['CXXCPP'] + '\n')
  if 'CXXFLAGS' in os.environ:
    out_file.write('export CXXFLAGS=' + os.environ['CXXFLAGS'] + '\n')

  # If linker environment is set, export them
  if 'LDFLAGS' in os.environ:
    out_file.write('export LDFLAGS=' + os.environ['LDFLAGS'] + '\n')
  if 'LIBS' in os.environ:
    out_file.write('export LIBS=' + os.environ['LIBS'] + '\n')

  # Invoke the programs
  out_file.write('# Execute the input programs\n')
  out_file.write('$*')

  make_executable(env_exec_file)
  print 'Wrote the environment exec file:  \t"%s"' % (env_exec_file)


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

  # Discover the tools environment
  env_map['CC'] = discover_tool('gcc',"C compiler", 'CC')
  env_map['CXX'] = discover_tool('g++',"C++ compiler", 'CXX')
  env_map['CPP'] = discover_tool('cpp',"C preprocessor", 'CPP')
  env_map['CXXCPP'] = discover_tool('cpp',"C++ preprocessor", 'CXXCPP')
  env_map['LD'] =  discover_tool('ld',"linker", 'LD')
  env_map['BLDFLAG'] = discover_linker(env_map)

  # Discover the utilities
  env_map['AUTOMAKE'] = discover_tool('automake', "Automake")
  env_map['AUTOCONF'] = discover_tool('autoconf', "Autoconf")
  env_map['MAKE'] = discover_tool('make', "Make")
  env_map['CMAKE'] = discover_tool('cmake', "CMake")
  env_map['PYTHON2'] = discover_tool('python2', "Python2")

  if platform == "Darwin":
    env_map['AR'] = discover_tool('libtool', "archiver", 'AR')
  else:
    env_map['AR'] = discover_tool('ar', "archiver", 'AR')

  env_map['GCOV']= discover_tool('gcov',"coverage tool", 'GCOV')
  env_map['DWP'] = discover_tool_default('dwp', "dwp", 'DWP', '/usr/bin/dwp')
  env_map['NM'] = discover_tool_default('nm', "nm", 'NM', '/usr/bin/nm')
  env_map['OBJCOPY'] = discover_tool_default('objcopy', "objcopy", 'OBJCOPY', '/usr/bin/objcopy')
  env_map['OBJDUMP'] = discover_tool_default('objdump', "objdump", 'OBJDUMP', '/usr/bin/objdump')
  env_map['STRIP'] = discover_tool_default('strip', "strip", 'STRIP', '/usr/bin/strip')

  # write the environment executable file
  write_env_exec_file(platform, env_map)

if __name__ == '__main__':
  main()
