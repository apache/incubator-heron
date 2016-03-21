#!/usr/bin/python2.7

import argparse
import atexit
import base64
import contextlib
import glob
import logging
import logging.handlers
import os
import shutil
import sys
import subprocess
import tarfile
import tempfile

from heron.proto import topology_pb2
import heron.cli3.src.python.args as args
import heron.cli3.src.python.execute as execute
import heron.cli3.src.python.jars as jars
import heron.cli3.src.python.opts as opts
import heron.cli3.src.python.utils as utils

################################################################################
# Create a subparser for the submit command
################################################################################
def create_parser(subparsers):
  parser = subparsers.add_parser(
      'submit',
      help='Submit a topology',
      usage = "%(prog)s [options] cluster/[role]/[environ] " + \
              "topology-file-name topology-class-name [topology-args]",
      add_help = False
  )

  args.add_titles(parser)
  args.add_cluster_role_env(parser)
  args.add_topology_file(parser)
  args.add_topology_class(parser)
  args.add_config(parser)

  parser.add_argument(
      '--deploy-deactivated',
      metavar='(a boolean; default: "false")',
      default=False)

  args.add_verbose(parser)
  args.add_trace_execution(parser)

  parser.set_defaults(subcommand='submit')
  return parser

################################################################################
# Launch a topology given topology jar, its definition file and configurations
################################################################################
def launch_a_topology(cluster_role_env, tmp_dir, topology_file, topology_defn_file,
        config_path, config_overrides):

  # get the normalized path for topology.tar.gz
  topology_pkg_path = utils.normalized_class_path(os.path.join(tmp_dir, 'topology.tar.gz'))

  # TO DO - when you give a config path - the name of the directory might be
  # different - need to change the name to conf

  # create a tar package with
  tar_pkg_files = [topology_file, topology_defn_file]
  utils.create_tar(topology_pkg_path, tar_pkg_files, config_path)

  # pass the args to submitter main
  args = [
      "--cluster", cluster_role_env[0],
      "--role", cluster_role_env[1],
      "--environment", cluster_role_env[2],
      "--heron_home", utils.get_heron_dir(),
      "--config_path", config_path,
      "--config_overrides", base64.b64encode(config_overrides),
      "--topology_package", topology_pkg_path,
      "--topology_defn", topology_defn_file,
      "--topology_jar", topology_file
  ]

  lib_jars = utils.get_heron_libs(
      jars.scheduler_jars() + jars.uploader_jars() + jars.statemgr_jars() + jars.packing_jars()
  )

  # invoke the submitter to submit and launch the topology
  execute.heron_class(
      'com.twitter.heron.scheduler.SubmitterMain',
      lib_jars,
      extra_jars=[],
      args = args
  )

################################################################################
# Launch topologies
################################################################################
def launch_topologies(cluster_role_env, topology_file, tmp_dir, config_path,
        config_overrides):

  # the submitter would have written the .defn file to the tmp_dir
  defn_files = glob.glob(tmp_dir + '/*.defn')

  if len(defn_files) == 0:
    raise Exception("No topologies found")

  try:
    for defn_file in defn_files:

      # load the topology definition from the file
      topology_defn = topology_pb2.Topology()
      try:
        f = open(defn_file, "rb")
        topology_defn.ParseFromString(f.read())
        f.close()

      except:
        raise Exception("Could not open and parse topology defn file %s" % defn_file)

      # launch the topology
      try:
        print "Launching topology \'%s\'" % topology_defn.name
        launch_a_topology(cluster_role_env, tmp_dir, topology_file, defn_file,
            config_path, config_overrides)

        print "Topology \'%s\' launched successfully" % topology_defn.name

      except Exception as ex:
        print 'Failed to launch topology \'%s\' because %s' % (topology_defn.name, str(ex))
        raise

  except:
    raise

################################################################################
# We use the packer to make a package for the jar and dump it
# to a well-known location. We then run the main method of class
# with the specified arguments. We pass arguments as heron.options.
#
# This will run the jar file with the topology_class_name. The HeronSubmitter
# inside will write out the topology defn file to a location that
# we specify. Then we write the topology defn file to a well known
# packer location. We then write to appropriate places in zookeeper
# and launch the aurora jobs
################################################################################
def submit_fatjar(command, parser, cl_args, unknown_args):
  try:

    # extract the necessary arguments
    cluster_role_env = cl_args['cluster/[role]/[env]']
    topology_file = cl_args['topology-file-name']
    topology_class_name = cl_args['topology-class-name']
    topology_args = tuple(unknown_args)

    # extract the config path
    config_path = cl_args['config_path']

  except KeyError:
    # if some of the arguments are not found, print error and exit
    subparser = utils.get_subparser(parser, command)
    print(subparser.format_help())
    parser.exit()

  config_path = utils.get_heron_cluster_conf_dir(cluster_role_env, config_path);
  if not os.path.isdir(config_path):
    print("Config directory does not exist: %s" % config_path);
    parser.exit();

  # create a temporary directory for topology definition file
  tmp_dir = tempfile.mkdtemp()

  # if topology needs to be launched in deactivated state, do it so
  if cl_args['deploy_deactivated']:
    initial_state = topology_pb2.TopologyState.Name(topology_pb2.PAUSED)
  else:
    initial_state = topology_pb2.TopologyState.Name(topology_pb2.RUNNING)

  # set the tmp dir and deactivated state in global options
  opts.set_config('cmdline.topologydefn.tmpdirectory', tmp_dir)
  opts.set_config('cmdline.topology.initial.state', initial_state)

  # execute main of the topology to create the topology definition
  execute.heron_class(
      topology_class_name,
      utils.get_heron_libs(jars.topology_jars()),
      extra_jars = [topology_file],
      args = topology_args
  )

  try:
    cluster_role_env = utils.parse_cluster_role_env(cluster_role_env)
    config_overrides = utils.parse_cmdline_override(cl_args)

    launch_topologies(cluster_role_env, topology_file, tmp_dir, config_path,
        config_overrides)

  finally:
    shutil.rmtree(tmp_dir)

################################################################################
# Extract and execute the java files inside the tar and then add topology
# definition file created by running submitTopology
#
# We use the packer to make a package for the tar and dump it
# to a well-known location. We then run the main method of class
# with the specified arguments. We pass arguments as heron.options.
# This will run the jar file with the topology class name.
#
# The HeronSubmitter inside will write out the topology defn file to a location
# that we specify. Then we write the topology defn file to a well known
# packer location. We then write to appropriate places in zookeeper
# and launch the aurora jobs
################################################################################
def submit_tar(command, parser, cl_args, unknown_args):
  try:
    cluster_role_env = cl_args['cluster/[role]/[env]']
    topology_file = cl_args['topology-file-name']
    topology_class_name = cl_args['topology-class-name']
    topology_args = tuple(unknown_args)

    config_path = cl_args['config_path']

  except KeyError:
    subparser = utils.get_subparser(parser, command)
    print(subparser.format_help())
    parser.exit()

  config_path = utils.get_heron_cluster_conf_dir(cluster_role_env, config_path);
  if not os.path.isdir(config_path):
    print("Config directory does not exist: %s" % config_path);
    parser.exit();

  tmp_dir = tempfile.mkdtemp()
  opts.set_config('cmdline.topologydefn.tmpdirectory', tmp_dir)

  if cl_args['deploy_deactivated']:
    initial_state = topology_pb2.TopologyState.Name(topology_pb2.PAUSED)
  else:
    initial_state = topology_pb2.TopologyState.Name(topology_pb2.RUNNING)

  opts.set_config('cmdline.topology.initial.state', initial_state)

  execute.heron_tar(topology_class_name, topology_file, topology_args, tmp_dir)

  try:
    cluster_role_env = utils.parse_cluster_role_env(cluster_role_env)
    config_overrides = utils.parse_cmdline_override(cl_args)

    launch_topologies(cluster_role_env, topology_file, tmp_dir, config_path, config_overrides)

  finally:
    shutil.rmtree(tmp_dir)

################################################################################
#  Submits the topology to the scheduler
#  * Depending on the topology file name extension, we treat the file as a
#    fatjar (if the ext is .jar) or a tar file (if the ext is .tar/.tar.gz).
#  * We upload the topology file to the packer, update zookeeper and launch
#    aurora jobs representing that topology
#  * You can see your topology in Heron UI
################################################################################
def run(command, parser, cl_args, unknown_args):

  # get the topology file name
  topology_file = cl_args['topology-file-name']

  # check to see if the topology file exists
  if not os.path.isfile(topology_file):
    print "Topology jar/tar %s does not exist" % topology_file
    sys.exit(1)

  # check the extension of the file name to see if it is tar/jar file.
  if topology_file.endswith(".jar"):
    submit_fatjar(command, parser, cl_args, unknown_args)

  elif topology_file.endswith(".tar") or topology_file.endswith(".tar.gz"):
    submit_tar(command, parser, cl_args, unknown_args)
  else:
    print "Unknown file type. Please use .tar or .jar file"
    sys.exit(1)

