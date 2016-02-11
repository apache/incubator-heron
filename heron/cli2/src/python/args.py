import os
import argparse
import getpass

import heron.cli2.src.python.utils as utils

def add_titles(parser):
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"  
  return parser

def insert_bool(param, command_args):
  index = 0 
  found = False
  for lelem in command_args:
    if lelem == '--' and not found:
      break
    if lelem == param:
      found = True
      break
    index = index + 1   

  if found:
    command_args.insert(index + 1, 'True')
  return command_args 

def insert_bool_values(command_line_args):
  args1 = insert_bool('--verbose', command_line_args)
  args2 = insert_bool('--deploy-deactivated', args1)
  args3 = insert_bool('--trace-execution', args2)
  return args2

def add_verbose(parser):
  parser.add_argument(
      '--verbose',
      metavar='(a boolean; default: "false")',
      default=False)
  return parser

def add_trace_execution(parser):
  parser.add_argument(
      '--trace-execution',
      metavar='(a boolean; default: "false")',
      default=False)
  return parser

def add_topology(parser):
  parser.add_argument(
      'topology-name',
      help='Name of the topology')
  return parser

def add_topology_file(parser):
  parser.add_argument(
      'topology-file-name',
      help='Topology jar/tar/zip file')
  return parser

def add_topology_class(parser):
  parser.add_argument(
      'topology-class-name',
      help='Topology class name')
  return parser

def add_cluster_role_env(parser):
  parser.add_argument(
      'cluster/[role]/[env]',
       help='Cluster, role, and environ to run topology'
  )
  return parser

def add_config(parser):
  default_config_path = 'conf/com/twitter/aurora'
  parser.add_argument(
      '--config-path',
      metavar='(a string; path to scheduler config; default: "' + default_config_path + '")',
      default=os.path.join(utils.get_heron_dir(), default_config_path))

  default_config_loader = 'com.twitter.heron.scheduler.aurora.AuroraConfigLoader'
  parser.add_argument(
      '--config-loader',
      metavar='(a string; class to load scheduler config; default: "' + default_config_loader + '")',
      default=default_config_loader)

  parser.add_argument(
      '--config-property',
      metavar='(a string; scheduler config property; default: [])',
      action='append',
      default=[])
  return parser

################################################################################
# Parse the cluster/[role]/[environ], supply defaults, if not provided
################################################################################
def parse_cluster_role_env(cluster_role_env):
  parts = cluster_role_env.split('/')[:3]

  # if role is not provided, use username instead
  if len(parts) == 1:
    parts.append(getpass.getuser())

  # if environ is not provided, use 'default'
  if len(parts) == 2:
    parts.append('default')

  # if cluster or role or environ is empty, print
  if len(parts[0]) == 0 or len(parts[1]) == 0 or len(parts[2]) == 0:
    print "Failed to parse %s: %s" % (argstr, namespace[argstr])
    sys.exit(1)

  return "cluster=%s role=%s environ=%s" % (parts[0], parts[1], parts[2])

################################################################################
# Parse the command line for overriding the defaults
################################################################################
def parse_cmdline_override(namespace):
  override = []
  for key in namespace.keys():
    # Notice we could not use "if not namespace[key]",
    # since it would filter out 0 too, rather than just "None"
    if namespace[key] is None:
      continue
    property_key = key.replace('-', '.').replace('_', '.')
    property_value = str(namespace[key])
    override.append('%s="%s"' % (property_key, property_value))
  return ' '.join(override)
