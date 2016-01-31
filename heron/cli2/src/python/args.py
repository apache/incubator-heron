import os
import argparse

import heron.cli2.src.python.utils as utils

def add_titles(parser):
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"  
  return parser

def add_verbose(parser):
  parser.add_argument(
      '--verbose',
      metavar='(a boolean; default: "false")',
      default=False)
  return parser

def add_topology(parser):
  parser.add_argument(
      'topology-name',
      help='Name of the topology'
  )
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

def add_config_overrides(parser):
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
