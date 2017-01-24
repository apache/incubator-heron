# Copyright 2016 Twitter. All rights reserved.
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
''' submit.py '''
import glob
import logging
import os
import shutil
import tempfile

from heron.common.src.python.utils.log import Log
from heron.proto import topology_pb2
from heron.tools.cli.src.python.response import Response, Status
import heron.tools.cli.src.python.args as cli_args
import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.cli.src.python.opts as opts
import heron.tools.common.src.python.utils.config as config
import heron.tools.common.src.python.utils.classpath as classpath

# pylint: disable=too-many-return-statements

################################################################################
def create_parser(subparsers):
  '''
  Create a subparser for the submit command
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'submit',
      help='Submit a topology',
      usage="%(prog)s [options] cluster/[role]/[env] " + \
            "topology-file-name topology-class-name [topology-args]",
      add_help=False
  )

  cli_args.add_titles(parser)
  cli_args.add_cluster_role_env(parser)
  cli_args.add_topology_file(parser)
  cli_args.add_topology_class(parser)
  cli_args.add_config(parser)
  cli_args.add_deactive_deploy(parser)
  cli_args.add_extra_launch_classpath(parser)
  cli_args.add_system_property(parser)
  cli_args.add_dry_run(parser)
  cli_args.add_verbose(parser)

  parser.set_defaults(subcommand='submit')
  return parser


################################################################################
def launch_a_topology(cl_args, tmp_dir, topology_file, topology_defn_file, topology_name):
  '''
  Launch a topology given topology jar, its definition file and configurations
  :param cl_args:
  :param tmp_dir:
  :param topology_file:
  :param topology_defn_file:
  :return:
  '''
  # get the normalized path for topology.tar.gz
  topology_pkg_path = config.normalized_class_path(os.path.join(tmp_dir, 'topology.tar.gz'))

  # get the release yaml file
  release_yaml_file = config.get_heron_release_file()

  # create a tar package with the cluster configuration and generated config files
  config_path = cl_args['config_path']
  tar_pkg_files = [topology_file, topology_defn_file]
  generated_config_files = [release_yaml_file, cl_args['override_config_file']]

  config.create_tar(topology_pkg_path, tar_pkg_files, config_path, generated_config_files)

  # pass the args to submitter main
  args = [
      "--cluster", cl_args['cluster'],
      "--role", cl_args['role'],
      "--environment", cl_args['environ'],
      "--heron_home", config.get_heron_dir(),
      "--config_path", config_path,
      "--override_config_file", cl_args['override_config_file'],
      "--release_file", release_yaml_file,
      "--topology_package", topology_pkg_path,
      "--topology_defn", topology_defn_file,
      "--topology_bin", topology_file   # pex file if pex specified
  ]

  if Log.getEffectiveLevel() == logging.DEBUG:
    args.append("--verbose")

  if cl_args["dry_run"]:
    args.append("--dry_run")

  if "dry_run_format" in cl_args:
    args += ["--dry_run_format", cl_args["dry_run_format"]]

  lib_jars = config.get_heron_libs(
      jars.scheduler_jars() + jars.uploader_jars() + jars.statemgr_jars() + jars.packing_jars()
  )
  extra_jars = cl_args['extra_launch_classpath'].split(':')

  # invoke the submitter to submit and launch the topology
  main_class = 'com.twitter.heron.scheduler.SubmitterMain'
  resp = execute.heron_class(
      class_name=main_class,
      lib_jars=lib_jars,
      extra_jars=extra_jars,
      args=args,
      java_defines=[])
  err_context = "Failed to launch topology '%s'" % topology_name
  if cl_args["dry_run"]:
    err_context += " in dry-run mode"
  succ_context = "Successfully launched topology '%s'" % topology_name
  if cl_args["dry_run"]:
    succ_context += " in dry-run mode"
  resp.add_context(err_context, succ_context)
  return resp

################################################################################
def launch_topologies(cl_args, topology_file, tmp_dir):
  '''
  Launch topologies
  :param cl_args:
  :param topology_file:
  :param tmp_dir:
  :return: list(Responses)
  '''
  # the submitter would have written the .defn file to the tmp_dir
  defn_files = glob.glob(tmp_dir + '/*.defn')

  if len(defn_files) == 0:
    return Response(Status.HeronError, "No topologies found under %s" % tmp_dir)

  responses = []
  for defn_file in defn_files:
    # load the topology definition from the file
    topology_defn = topology_pb2.Topology()
    try:
      handle = open(defn_file, "rb")
      topology_defn.ParseFromString(handle.read())
      handle.close()
    except Exception as e:
      msg = "Cannot load topology definition '%s'" % defn_file
      return Response(Status.HeronError, msg, str(e))

    # launch the topology
    Log.info("Launching topology: \'%s\'", topology_defn.name)
    resp = launch_a_topology(
        cl_args, tmp_dir, topology_file, defn_file, topology_defn.name)
    responses.append(resp)
  return responses


################################################################################
def submit_fatjar(cl_args, unknown_args, tmp_dir):
  '''
  We use the packer to make a package for the jar and dump it
  to a well-known location. We then run the main method of class
  with the specified arguments. We pass arguments as an environment variable HERON_OPTIONS.

  This will run the jar file with the topology_class_name. The submitter
  inside will write out the topology defn file to a location that
  we specify. Then we write the topology defn file to a well known
  location. We then write to appropriate places in zookeeper
  and launch the scheduler jobs
  :param cl_args:
  :param unknown_args:
  :param tmp_dir:
  :return:
  '''
  # execute main of the topology to create the topology definition
  topology_file = cl_args['topology-file-name']

  main_class = cl_args['topology-class-name']
  resp = execute.heron_class(
      class_name=main_class,
      lib_jars=config.get_heron_libs(jars.topology_jars()),
      extra_jars=[topology_file],
      args=tuple(unknown_args),
      java_defines=cl_args['topology_main_jvm_property'])

  if resp.status != Status.Ok:
    err_context = "Failed to create topology definition \
      file when executing class '%s' of file '%s'" % (main_class, topology_file)
    resp.add_context(err_context)
    return resp

  responses = launch_topologies(cl_args, topology_file, tmp_dir)
  shutil.rmtree(tmp_dir)

  return responses


################################################################################
def submit_tar(cl_args, unknown_args, tmp_dir):
  '''
  Extract and execute the java files inside the tar and then add topology
  definition file created by running submitTopology

  We use the packer to make a package for the tar and dump it
  to a well-known location. We then run the main method of class
  with the specified arguments. We pass arguments as an environment variable HERON_OPTIONS.
  This will run the jar file with the topology class name.

  The submitter inside will write out the topology defn file to a location
  that we specify. Then we write the topology defn file to a well known
  packer location. We then write to appropriate places in zookeeper
  and launch the aurora jobs
  :param cl_args:
  :param unknown_args:
  :param tmp_dir:
  :return:
  '''
  # execute main of the topology to create the topology definition
  topology_file = cl_args['topology-file-name']
  java_defines = cl_args['topology_main_jvm_property']
  main_class = cl_args['topology-class-name']
  resp = execute.heron_tar(
      main_class,
      topology_file,
      tuple(unknown_args),
      tmp_dir,
      java_defines)

  if resp.status != Status.Ok:
    err_context = "Failed to create topology definition \
      file when executing class '%s' of file '%s'" % (main_class, topology_file)
    resp.add_context(err_context)
    return resp

  responses = launch_topologies(cl_args, topology_file, tmp_dir)
  shutil.rmtree(tmp_dir)

  return responses

################################################################################
#  Execute the pex file to create topology definition file by running
#  the topology's main class.
################################################################################
# pylint: disable=unused-argument
def submit_pex(cl_args, unknown_args, tmp_dir):
  # execute main of the topology to create the topology definition
  topology_file = cl_args['topology-file-name']
  topology_class_name = cl_args['topology-class-name']
  resp = execute.heron_pex(
      topology_file, topology_class_name, tuple(unknown_args))
  if resp.status != Status.Ok:
    err_context = "Failed to create topology definition \
      file when executing class '%s' of file '%s'" % (topology_class_name, topology_file)
    resp.add_context(err_context)
    return resp

  responses = launch_topologies(cl_args, topology_file, tmp_dir)
  shutil.rmtree(tmp_dir)

  return responses

################################################################################
# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  '''
  Submits the topology to the scheduler
    * Depending on the topology file name extension, we treat the file as a
      fatjar (if the ext is .jar) or a tar file (if the ext is .tar/.tar.gz).
    * We upload the topology file to the packer, update zookeeper and launch
      scheduler jobs representing that topology
    * You can see your topology in Heron UI
  :param command:
  :param parser:
  :param cl_args:
  :param unknown_args:
  :return:
  '''
  # get the topology file name
  topology_file = cl_args['topology-file-name']

  # check to see if the topology file exists
  if not os.path.isfile(topology_file):
    err_msg = "Topology jar|tar|pex file '%s' does not exist" % topology_file
    return Response(Status.InvocationError, detailed_msg=err_msg)

  # check if it is a valid file type
  jar_type = topology_file.endswith(".jar")
  tar_type = topology_file.endswith(".tar") or topology_file.endswith(".tar.gz")
  pex_type = topology_file.endswith(".pex")
  if not jar_type and not tar_type and not pex_type:
    ext_name = os.path.splitext(topology_file)
    err_msg = "Unknown file type '%s'. Please use .tar or .tar.gz or .jar or .pex file" % ext_name
    return Response(Status.InvocationError, detailed_msg=err_msg)

  # check if extra launch classpath is provided and if it is validate
  if cl_args['extra_launch_classpath']:
    valid_classpath = classpath.valid_java_classpath(cl_args['extra_launch_classpath'])
    if not valid_classpath:
      err_msg = "One of jar or directory in extra launch classpath does not exist: %s" % \
        cl_args['extra_launch_classpath']
      return Response(Status.InvocationError, detailed_msg=err_msg)

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

  # check the extension of the file name to see if it is tar/jar file.
  if jar_type:
    return submit_fatjar(cl_args, unknown_args, tmp_dir)

  elif tar_type:
    return submit_tar(cl_args, unknown_args, tmp_dir)

  else:
    return submit_pex(cl_args, unknown_args, tmp_dir)

