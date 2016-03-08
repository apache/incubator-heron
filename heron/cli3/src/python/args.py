import os
import argparse

import heron.cli3.src.python.utils as utils

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

  # the default config path
  default_config_path = utils.get_heron_conf_dir()

  parser.add_argument(
      '--config-path',
      metavar='(a string; path to cluster config; default: "' + default_config_path + '/<cluster>")',
      default=os.path.join(utils.get_heron_dir(), default_config_path))

  parser.add_argument(
      '--config-property',
      metavar='(a string; a config property; default: [])',
      action='append',
      default=[])
  return parser
