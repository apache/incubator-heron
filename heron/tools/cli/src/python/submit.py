#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

''' submit.py '''
from future.standard_library import install_aliases
install_aliases()

import glob
import logging
import os
import tempfile
import subprocess
from urllib.parse import urlparse
import requests

from heron.common.src.python.utils.log import Log
from heron.proto import topology_pb2
from heron.tools.cli.src.python.result import SimpleResult, Status
import heron.tools.cli.src.python.args as cli_args
import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.cli.src.python.opts as opts
import heron.tools.cli.src.python.result as result
import heron.tools.cli.src.python.rest as rest
import heron.tools.common.src.python.utils.config as config
import heron.tools.common.src.python.utils.classpath as classpath

# pylint: disable=too-many-return-statements

################################################################################
def launch_mode_msg(cl_args):
  '''
  Depending on the mode of launching a topology provide a message
  :param cl_args:
  :return:
  '''
  if cl_args['dry_run']:
    return "in dry-run mode"
  return ""

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
      add_help=True
  )

  cli_args.add_titles(parser)
  cli_args.add_cluster_role_env(parser)
  cli_args.add_topology_file(parser)
  cli_args.add_topology_class(parser)
  cli_args.add_config(parser)
  cli_args.add_deactive_deploy(parser)
  cli_args.add_dry_run(parser)
  cli_args.add_extra_launch_classpath(parser)
  cli_args.add_release_yaml_file(parser)
  cli_args.add_service_url(parser)
  cli_args.add_system_property(parser)
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
  :param topology_name:
  :return:
  '''
  # get the normalized path for topology.tar.gz
  topology_pkg_path = config.normalized_class_path(os.path.join(tmp_dir, 'topology.tar.gz'))

  # get the release yaml file
  release_yaml_file = cl_args['release_yaml_file']

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
      "--submit_user", cl_args['submit_user'],
      "--heron_home", config.get_heron_dir(),
      "--config_path", config_path,
      "--override_config_file", cl_args['override_config_file'],
      "--release_file", release_yaml_file,
      "--topology_package", topology_pkg_path,
      "--topology_defn", topology_defn_file,
      "--topology_bin", os.path.basename(topology_file)   # pex/cpp file if pex/cpp specified
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
  main_class = 'org.apache.heron.scheduler.SubmitterMain'
  res = execute.heron_class(
      class_name=main_class,
      lib_jars=lib_jars,
      extra_jars=extra_jars,
      args=args,
      java_defines=[])

  err_ctxt = "Failed to launch topology '%s' %s" % (topology_name, launch_mode_msg(cl_args))
  succ_ctxt = "Successfully launched topology '%s' %s" % (topology_name, launch_mode_msg(cl_args))

  res.add_context(err_ctxt, succ_ctxt)
  return res

################################################################################
# pylint: disable=superfluous-parens
def launch_topology_server(cl_args, topology_file, topology_defn_file, topology_name):
  '''
  Launch a topology given topology jar, its definition file and configurations
  :param cl_args:
  :param topology_file:
  :param topology_defn_file:
  :param topology_name:
  :return:
  '''
  service_apiurl = cl_args['service_url'] + rest.ROUTE_SIGNATURES['submit'][1]
  service_method = rest.ROUTE_SIGNATURES['submit'][0]
  data = dict(
      name=topology_name,
      cluster=cl_args['cluster'],
      role=cl_args['role'],
      environment=cl_args['environ'],
      user=cl_args['submit_user'],
  )

  Log.info("" + str(cl_args))
  overrides = dict()
  if 'config_property' in cl_args:
    overrides = config.parse_override_config(cl_args['config_property'])

  if overrides:
    data.update(overrides)

  if cl_args['dry_run']:
    data["dry_run"] = True

  files = dict(
      definition=open(topology_defn_file, 'rb'),
      topology=open(topology_file, 'rb'),
  )

  err_ctxt = "Failed to launch topology '%s' %s" % (topology_name, launch_mode_msg(cl_args))
  succ_ctxt = "Successfully launched topology '%s' %s" % (topology_name, launch_mode_msg(cl_args))

  try:
    r = service_method(service_apiurl, data=data, files=files)
    ok = r.status_code is requests.codes.ok
    created = r.status_code is requests.codes.created
    s = Status.Ok if created or ok else Status.HeronError
    if s is Status.HeronError:
      Log.error(r.json().get('message', "Unknown error from API server %d" % r.status_code))
    elif ok:
      # this case happens when we request a dry_run
      print(r.json().get("response"))
  except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as err:
    Log.error(err)
    return SimpleResult(Status.HeronError, err_ctxt, succ_ctxt)
  return SimpleResult(s, err_ctxt, succ_ctxt)


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
    return SimpleResult(Status.HeronError, "No topologies found under %s" % tmp_dir)

  results = []
  for defn_file in defn_files:
    # load the topology definition from the file
    topology_defn = topology_pb2.Topology()
    try:
      handle = open(defn_file, "rb")
      topology_defn.ParseFromString(handle.read())
      handle.close()
    except Exception as e:
      err_context = "Cannot load topology definition '%s': %s" % (defn_file, e)
      return SimpleResult(Status.HeronError, err_context)

    # log topology and components configurations
    Log.debug("Topology config: %s", topology_defn.topology_config)
    Log.debug("Component config:")
    for spout in topology_defn.spouts:
      Log.debug("%s => %s", spout.comp.name, spout.comp.config)
    for bolt in topology_defn.bolts:
      Log.debug("%s => %s", bolt.comp.name, bolt.comp.config)

    # launch the topology
    Log.info("Launching topology: \'%s\'%s", topology_defn.name, launch_mode_msg(cl_args))

    # check if we have to do server or direct based deployment
    if cl_args['deploy_mode'] == config.SERVER_MODE:
      res = launch_topology_server(
          cl_args, topology_file, defn_file, topology_defn.name)
    else:
      res = launch_a_topology(
          cl_args, tmp_dir, topology_file, defn_file, topology_defn.name)
    results.append(res)

  return results


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

  res = execute.heron_class(
      class_name=main_class,
      lib_jars=config.get_heron_libs(jars.topology_jars()),
      extra_jars=[topology_file],
      args=tuple(unknown_args),
      java_defines=cl_args['topology_main_jvm_property'])

  result.render(res)

  if not result.is_successful(res):
    err_context = ("Failed to create topology definition " \
      "file when executing class '%s' of file '%s'") % (main_class, topology_file)
    res.add_context(err_context)
    return res

  results = launch_topologies(cl_args, topology_file, tmp_dir)

  return results


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
  res = execute.heron_tar(
      main_class,
      topology_file,
      tuple(unknown_args),
      tmp_dir,
      java_defines)

  result.render(res)

  if not result.is_successful(res):
    err_context = ("Failed to create topology definition " \
      "file when executing class '%s' of file '%s'") % (main_class, topology_file)
    res.add_context(err_context)
    return res

  return launch_topologies(cl_args, topology_file, tmp_dir)

################################################################################
#  Execute the pex file to create topology definition file by running
#  the topology's main class.
################################################################################
# pylint: disable=unused-argument
def submit_pex(cl_args, unknown_args, tmp_dir):
  # execute main of the topology to create the topology definition
  topology_file = cl_args['topology-file-name']
  topology_class_name = cl_args['topology-class-name']
  res = execute.heron_pex(
      topology_file, topology_class_name, tuple(unknown_args))

  result.render(res)
  if not result.is_successful(res):
    err_context = ("Failed to create topology definition " \
      "file when executing class '%s' of file '%s'") % (topology_class_name, topology_file)
    res.add_context(err_context)
    return res

  return launch_topologies(cl_args, topology_file, tmp_dir)

################################################################################
#  Execute the cpp file to create topology definition file by running
#  the topology's binary.
################################################################################
# pylint: disable=unused-argument
def submit_cpp(cl_args, unknown_args, tmp_dir):
  # execute main of the topology to create the topology definition
  topology_file = cl_args['topology-file-name']
  topology_binary_name = cl_args['topology-class-name']
  res = execute.heron_cpp(topology_binary_name, tuple(unknown_args))

  result.render(res)
  if not result.is_successful(res):
    err_context = ("Failed to create topology definition " \
      "file when executing cpp binary '%s'") % (topology_binary_name)
    res.add_context(err_context)
    return res

  return launch_topologies(cl_args, topology_file, tmp_dir)

def download(uri, cluster):
  tmp_dir = tempfile.mkdtemp()
  cmd_downloader = config.get_heron_bin_dir() + "/heron-downloader.sh"
  cmd_uri = "-u " + uri
  cmd_destination = "-f " + tmp_dir
  cmd_heron_root = "-d " + config.get_heron_dir()
  cmd_heron_config = "-p " + config.get_heron_cluster_conf_dir(cluster, config.get_heron_conf_dir())
  cmd_mode = "-m local"
  cmd = [cmd_downloader, cmd_uri, cmd_destination, cmd_heron_root, cmd_heron_config, cmd_mode]
  Log.debug("download uri command: %s", cmd)
  subprocess.call(cmd)
  suffix = (".jar", ".tar", ".tar.gz", ".pex", ".dylib", ".so")
  for f in os.listdir(tmp_dir):
    if f.endswith(suffix):
      return os.path.join(tmp_dir, f)

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
  Log.debug("Submit Args %s", cl_args)

  # get the topology file name
  topology_file = cl_args['topology-file-name']

  if urlparse(topology_file).scheme:
    cl_args['topology-file-name'] = download(topology_file, cl_args['cluster'])
    topology_file = cl_args['topology-file-name']
    Log.debug("download uri to local file: %s", topology_file)

  # check to see if the topology file exists
  if not os.path.isfile(topology_file):
    err_context = "Topology file '%s' does not exist" % topology_file
    return SimpleResult(Status.InvocationError, err_context)

  # check if it is a valid file type
  jar_type = topology_file.endswith(".jar")
  tar_type = topology_file.endswith(".tar") or topology_file.endswith(".tar.gz")
  pex_type = topology_file.endswith(".pex")
  cpp_type = topology_file.endswith(".dylib") or topology_file.endswith(".so")
  if not (jar_type or tar_type or pex_type or cpp_type):
    _, ext_name = os.path.splitext(topology_file)
    err_context = "Unknown file type '%s'. Please use .tar "\
                  "or .tar.gz or .jar or .pex or .dylib or .so file"\
                  % ext_name
    return SimpleResult(Status.InvocationError, err_context)

  # check if extra launch classpath is provided and if it is validate
  if cl_args['extra_launch_classpath']:
    valid_classpath = classpath.valid_java_classpath(cl_args['extra_launch_classpath'])
    if not valid_classpath:
      err_context = "One of jar or directory in extra launch classpath does not exist: %s" % \
        cl_args['extra_launch_classpath']
      return SimpleResult(Status.InvocationError, err_context)

  # create a temporary directory for topology definition file
  tmp_dir = tempfile.mkdtemp()
  opts.cleaned_up_files.append(tmp_dir)

  # if topology needs to be launched in deactivated state, do it so
  if cl_args['deploy_deactivated']:
    initial_state = topology_pb2.TopologyState.Name(topology_pb2.PAUSED)
  else:
    initial_state = topology_pb2.TopologyState.Name(topology_pb2.RUNNING)

  # set the tmp dir and deactivated state in global options
  opts.set_config('cmdline.topologydefn.tmpdirectory', tmp_dir)
  opts.set_config('cmdline.topology.initial.state', initial_state)
  opts.set_config('cmdline.topology.role', cl_args['role'])
  opts.set_config('cmdline.topology.environment', cl_args['environ'])
  opts.set_config('cmdline.topology.cluster', cl_args['cluster'])
  opts.set_config('cmdline.topology.file_name', cl_args['topology-file-name'])
  opts.set_config('cmdline.topology.class_name', cl_args['topology-class-name'])
  opts.set_config('cmdline.topology.submit_user', cl_args['submit_user'])

  # Use CLI release yaml file if the release_yaml_file config is empty
  if not cl_args['release_yaml_file']:
    cl_args['release_yaml_file'] = config.get_heron_release_file()

  # check the extension of the file name to see if it is tar/jar file.
  if jar_type:
    return submit_fatjar(cl_args, unknown_args, tmp_dir)
  elif tar_type:
    return submit_tar(cl_args, unknown_args, tmp_dir)
  elif cpp_type:
    return submit_cpp(cl_args, unknown_args, tmp_dir)
  else:
    return submit_pex(cl_args, unknown_args, tmp_dir)
