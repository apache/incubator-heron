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
import os
import shutil
import tempfile

from heron.common.src.python.color import Log
from heron.proto import topology_pb2

import heron.cli.src.python.args as cli_args
import heron.cli.src.python.execute as execute
import heron.cli.src.python.jars as jars
import heron.cli.src.python.opts as opts
import heron.common.src.python.utils as utils


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
  cli_args.add_system_property(parser)
  cli_args.add_verbose(parser)

  parser.set_defaults(subcommand='submit')
  return parser


################################################################################
def launch_a_topology(cl_args, tmp_dir, topology_file, topology_defn_file):
  '''
  Launch a topology given topology jar, its definition file and configurations
  :param cl_args:
  :param tmp_dir:
  :param topology_file:
  :param topology_defn_file:
  :return:
  '''
  # get the normalized path for topology.tar.gz
  topology_pkg_path = utils.normalized_class_path(os.path.join(tmp_dir, 'topology.tar.gz'))

  # get the release yaml file
  release_yaml_file = utils.get_heron_release_file()

  # create a tar package with the cluster configuration and generated config files
  config_path = cl_args['config_path']
  tar_pkg_files = [topology_file, topology_defn_file]
  generated_config_files = [release_yaml_file, cl_args['override_config_file']]

  utils.create_tar(topology_pkg_path, tar_pkg_files, config_path, generated_config_files)

  # pass the args to submitter main
  args = [
      "--cluster", cl_args['cluster'],
      "--role", cl_args['role'],
      "--environment", cl_args['environ'],
      "--heron_home", utils.get_heron_dir(),
      "--config_path", config_path,
      "--override_config_file", cl_args['override_config_file'],
      "--release_file", release_yaml_file,
      "--topology_package", topology_pkg_path,
      "--topology_defn", topology_defn_file,
      "--topology_jar", topology_file
  ]

  if opts.verbose():
    args.append("--verbose")

  lib_jars = utils.get_heron_libs(
      jars.scheduler_jars() + jars.uploader_jars() + jars.statemgr_jars() + jars.packing_jars()
  )

  # invoke the submitter to submit and launch the topology
  execute.heron_class(
      'com.twitter.heron.scheduler.SubmitterMain',
      lib_jars,
      extra_jars=[],
      args=args,
      java_defines=[]
  )


################################################################################
def launch_topologies(cl_args, topology_file, tmp_dir):
  '''
  Launch topologies
  :param cl_args:
  :param topology_file:
  :param tmp_dir:
  :return:
  '''
  # the submitter would have written the .defn file to the tmp_dir
  defn_files = glob.glob(tmp_dir + '/*.defn')

  if len(defn_files) == 0:
    raise Exception("No topologies found")

  try:
    for defn_file in defn_files:

      # load the topology definition from the file
      topology_defn = topology_pb2.Topology()
      try:
        handle = open(defn_file, "rb")
        topology_defn.ParseFromString(handle.read())
        handle.close()

      except:
        raise Exception("Could not open and parse topology defn file %s" % defn_file)

      # launch the topology
      try:
        Log.info("Launching topology \'%s\'" % topology_defn.name)
        launch_a_topology(cl_args, tmp_dir, topology_file, defn_file)
        Log.info("Topology \'%s\' launched successfully" % topology_defn.name)

      except Exception as ex:
        Log.exception('Failed to launch topology \'%s\' because %s' % (topology_defn.name, str(ex)))
        raise

  except:
    raise


################################################################################
def submit_fatjar(cl_args, unknown_args, tmp_dir):
  '''
   We use the packer to make a package for the jar and dump it
  to a well-known location. We then run the main method of class
  with the specified arguments. We pass arguments as heron.options.

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
  try:
    execute.heron_class(
        cl_args['topology-class-name'],
        utils.get_heron_libs(jars.topology_jars()),
        extra_jars=[topology_file],
        args=tuple(unknown_args),
        java_defines=cl_args['topology_main_jvm_property'])

  except Exception:
    Log.error("Unable to execute topology main class")
    return False

  try:
    launch_topologies(cl_args, topology_file, tmp_dir)

  except Exception:
    return False

  finally:
    shutil.rmtree(tmp_dir)

  return True


################################################################################
def submit_tar(cl_args, unknown_args, tmp_dir):
  '''
  Extract and execute the java files inside the tar and then add topology
  definition file created by running submitTopology

  We use the packer to make a package for the tar and dump it
  to a well-known location. We then run the main method of class
  with the specified arguments. We pass arguments as heron.options.
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
  execute.heron_tar(
      cl_args['topology-class-name'],
      topology_file,
      tuple(unknown_args),
      tmp_dir,
      java_defines)

  try:
    launch_topologies(cl_args, topology_file, tmp_dir)

  except Exception:
    return False

  finally:
    shutil.rmtree(tmp_dir)

  return True


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
    Log.error("Topology jar|tar file %s does not exist" % topology_file)
    return False

  # check if it is a valid file type
  jar_type = topology_file.endswith(".jar")
  tar_type = topology_file.endswith(".tar") or topology_file.endswith(".tar.gz")
  if not jar_type and not tar_type:
    Log.error("Unknown file type. Please use .tar or .tar.gz or .jar file")
    return False

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

  return False
