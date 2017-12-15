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
''' standalone.py '''
from subprocess import call
import subprocess
import sys
import os
import tempfile
import tarfile
import argparse
import socket
import requests
import time
import netifaces

from heron.common.src.python.utils.log import Log
from heron.tools.cli.src.python.result import SimpleResult, Status
import heron.tools.cli.src.python.args as cli_args
import heron.tools.common.src.python.utils.config as config

# pylint: disable=anomalous-backslash-in-string

class ACTION(object):
  SET = "set"
  CLUSTER = "cluster"
  TEMPLATE = "template"

TYPE = "type"

class SET(object):
  ZOOKEEPERS = "zookeepers"
  MASTERS = "masters"
  SLAVES = "slaves"

class CLUSTER(object):
  START = "start"
  STOP = "stop"
################################################################################
def create_parser(subparsers):
  '''
  Create a subparser for the standalone command
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'standalone',
      help='Start a standalone Heron cluster',
      add_help=True
  )

  cli_args.add_titles(parser)

  parser_action = parser.add_subparsers()

  parser_cluster = parser_action.add_parser(
      ACTION.CLUSTER,
      help='Start or stop cluster',
      add_help=True,
      formatter_class=argparse.RawTextHelpFormatter,
  )
  parser_cluster.set_defaults(action=ACTION.CLUSTER)

  parser_set = parser_action.add_parser(
      ACTION.SET,
      help='Set configurations for standalone cluster e.g. master or slave nodes',
      add_help=True,
      formatter_class=argparse.RawTextHelpFormatter
  )
  parser_set.set_defaults(action=ACTION.SET)

  parser_template = parser_action.add_parser(
      ACTION.TEMPLATE,
      help='Template Heron configurations based on cluster roles',
      add_help=True,
      formatter_class=argparse.RawTextHelpFormatter
  )
  parser_template.set_defaults(action=ACTION.TEMPLATE)

  parser_cluster.add_argument(
      TYPE,
      type=str,
      choices={CLUSTER.START, CLUSTER.STOP},
      help= \
"""
Choices supports the following:
  start     - Start standalone Heron cluster
  stop      - Stop standalone Heron cluster
"""
  )

  parser_set.add_argument(
      TYPE,
      type=str,
      choices={SET.MASTERS, SET.SLAVES, SET.ZOOKEEPERS},
      help=\
"""
Choices supports the following:
  masters     - Set the hostname/IP for master nodes
  slaves      - Set the hostname/IP for slave nodes
  zookeepers  - Set the hostname/IP for Zookeeper servers
"""
  )

  parser_template.add_argument(
      TYPE,
      type=str,
      choices={"configs"},
  )

  add_additional_args([parser_set, parser_cluster, parser_template])
  parser.set_defaults(subcommand='standalone')
  return parser


################################################################################
# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  '''
  runs parser
  '''

  action = cl_args["action"]
  action_type = cl_args["type"]
  if action == ACTION.SET:
    if action_type == SET.ZOOKEEPERS:
      call_editor(get_role_definition_file(SET.ZOOKEEPERS, cl_args))
      update_zookeeper_config_files(cl_args)
    elif action_type == SET.SLAVES:
      call_editor(get_role_definition_file(SET.SLAVES, cl_args))
      update_slave_config_files(cl_args)
    elif action_type == SET.MASTERS:
      call_editor(get_role_definition_file(SET.MASTERS, cl_args))
      update_master_config_files(cl_args)
    else:
      raise ValueError("Invalid set type %s" % action_type)
  elif action == ACTION.CLUSTER:
    if action_type == CLUSTER.START:
      start_cluster(cl_args)
    elif action_type == CLUSTER.STOP:
      if check_sure(cl_args, "Are you sure you want to stop the cluster?"
                             " This will terminate everything running in "
                             "the cluster and remove any scheduler state."):

        stop_cluster(cl_args)
    else:
      raise ValueError("Invalid cluster action %s" % action_type)
  elif action == ACTION.TEMPLATE:
    update_zookeeper_config_files(cl_args)
    update_master_config_files(cl_args)
    update_slave_config_files(cl_args)
  else:
    raise ValueError("Invalid action %s" % action)

  return SimpleResult(Status.Ok)

################################################################################
def update_slave_config_files(cl_args):
  '''
  update/template config files related to slave servers
  '''

  roles = read_and_parse_roles(cl_args)
  slaves = list(roles[SET.SLAVES])
  if not slaves:
    return
  Log.debug("Templating files for slaves...")

  # update apiserver location

  single_slave = slaves[0]
  uploader_config_template = "%s/standalone/templates/uploader.template.yaml" \
                             % cl_args["config_path"]
  with open(uploader_config_template, 'r') as tf:
    file_contents = tf.read()
    new_file_contents = file_contents.replace("<http_uploader_uri>",
                                              "http://%s:9000/api/v1/file/upload" % single_slave)

  uploader_config_actual = "%s/standalone/uploader.yaml" % cl_args["config_path"]
  with open(uploader_config_actual, 'w') as tf:
    tf.write(new_file_contents)
    tf.truncate()

  # Api server nomad job def
  apiserver_config_template = "%s/standalone/templates/apiserver.template.hcl" \
                              % cl_args["config_path"]
  with open(apiserver_config_template, 'r') as tf:
    file_contents = tf.read()
    new_file_contents = file_contents.replace(
        "<heron_apiserver_hostname>", '"%s"' % get_hostname(single_slave, cl_args))
    if is_self(single_slave):
      new_file_contents = new_file_contents.replace(
          "<heron_apiserver_executable>",
          '"%s/heron-apiserver"' % config.get_heron_bin_dir())
    else:
      new_file_contents = new_file_contents.replace(
          "<heron_apiserver_executable>",
          '"%s/.heron/bin/heron-apiserver"' % get_remote_home(single_slave, cl_args))

  apiserver_config_actual = "%s/standalone/resources/apiserver.hcl" % cl_args["config_path"]
  with open(apiserver_config_actual, 'w') as tf:
    tf.write(new_file_contents)
    tf.truncate()

def update_master_config_files(cl_args):
  '''
  update/template config files related to master servers
  '''
  roles = read_and_parse_roles(cl_args)
  masters = list(roles[SET.MASTERS])
  if not masters:
    return
  Log.debug("Templating files for masters...")
  slave_config_template = "%s/standalone/templates/slave.template.hcl" % cl_args["config_path"]
  new_file_contents = ""
  with open(slave_config_template, 'r') as tf:
    file_contents = tf.read()
    masters_in_quotes = ['"%s"' % master for master in masters]
    new_file_contents = file_contents.replace("<nomad_masters:master_port>",
                                              ", ".join(masters_in_quotes))

  slave_config_actual = "%s/standalone/resources/slave.hcl" % cl_args["config_path"]
  with open(slave_config_actual, 'w') as tf:
    tf.write(new_file_contents)
    tf.truncate()

  # template scheduler.yaml
  single_master = masters[0]
  scheduler_config_template = "%s/standalone/templates/scheduler.template.yaml" \
                               % cl_args["config_path"]
  with open(scheduler_config_template, 'r') as tf:
    file_contents = tf.read()
    new_file_contents = file_contents.replace("<scheduler_uri>",
                                              "http://%s:4646" % single_master)

  scheduler_config_actual = "%s/standalone/scheduler.yaml" % cl_args["config_path"]
  with open(scheduler_config_actual, 'w') as tf:
    tf.write(new_file_contents)
    tf.truncate()

def update_zookeeper_config_files(cl_args):
  '''
  update/template config files related to zookeeper servers
  '''
  roles = read_and_parse_roles(cl_args)
  zookeepers = ['"%s"' % zk if ":" in zk else '"%s:2181"' % zk for zk in roles[SET.ZOOKEEPERS]]
  if not zookeepers:
    return
  Log.debug("Templating files for zookeepers...")
  statemgr_config_file_template = "%s/standalone/templates/statemgr.template.yaml" \
                                  % cl_args["config_path"]
  new_file_contents = ""
  with open(statemgr_config_file_template, 'r') as tf:
    file_contents = tf.read()
    new_file_contents = file_contents.replace("<zookeeper_host:zookeeper_port>",
                                              ",".join(zookeepers))

  statemgr_config_file_actual = "%s/standalone/statemgr.yaml" % cl_args["config_path"]
  with open(statemgr_config_file_actual, 'w') as tf:
    tf.write(new_file_contents)
    tf.truncate()

def add_additional_args(parsers):
  '''
  add additional parameters to parser
  '''
  for parser in parsers:
    cli_args.add_verbose(parser)
    cli_args.add_config(parser)
    parser.add_argument(
        '--heron-dir',
        default=config.get_heron_dir(),
        help='Path to Heron home directory')

def stop_cluster(cl_args):
  '''
  teardown the cluster
  '''
  Log.info("Terminating cluster...")

  roles = read_and_parse_roles(cl_args)
  masters = roles[SET.MASTERS]
  slaves = roles[SET.SLAVES]
  dist_nodes = masters.union(slaves)

  # stop all jobs
  if masters:
    try:
      single_master = list(masters)[0]
      jobs = get_jobs(cl_args, single_master)
      for job in jobs:
        job_id = job["ID"]
        Log.info("Terminating job %s" % job_id)
        delete_job(cl_args, job_id, single_master)
    except:
      Log.debug("Error stopping jobs")
      Log.debug(sys.exc_info()[0])

  for node in dist_nodes:
    Log.info("Terminating processes on %s" % node)
    if not is_self(node):
      cmd = "ps aux | grep heron-nomad | awk '{print \$2}' " \
            "| xargs kill"
      cmd = ssh_remote_execute(cmd, node, cl_args)
    else:
      cmd = "ps aux | grep heron-nomad | awk '{print $2}' " \
            "| xargs kill"
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))

    Log.info("Cleaning up directories on %s" % node)
    cmd = "rm -rf /tmp/slave ; rm -rf /tmp/master"
    if not is_self(node):
      cmd = ssh_remote_execute(cmd, node, cl_args)
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))

def start_cluster(cl_args):
  '''
  Start a Heron standalone cluster
  '''
  roles = read_and_parse_roles(cl_args)
  masters = roles[SET.MASTERS]
  slaves = roles[SET.SLAVES]
  zookeepers = roles[SET.ZOOKEEPERS]
  Log.info("Roles:")
  Log.info(" - Master Servers: %s" % list(masters))
  Log.info(" - Slave Servers: %s" % list(slaves))
  Log.info(" - Zookeeper Servers: %s" % list(zookeepers))
  if not masters:
    Log.error("No master servers specified!")
    sys.exit(-1)
  if not slaves:
    Log.error("No slave servers specified!")
    sys.exit(-1)
  if not zookeepers:
    Log.error("No zookeeper servers specified!")
    sys.exit(-1)
  # make sure configs are templated
  update_zookeeper_config_files(cl_args)
  update_master_config_files(cl_args)
  update_slave_config_files(cl_args)

  dist_nodes = list(masters.union(slaves))
  # if just local deployment
  if not (len(dist_nodes) == 1 and is_self(dist_nodes[0])):
    distribute_package(roles, cl_args)
  start_master_nodes(masters, cl_args)
  start_slave_nodes(slaves, cl_args)
  start_api_server(masters, cl_args)
  Log.info("Heron standalone cluster complete!")

def start_api_server(masters, cl_args):
  '''
  Start the Heron API server
  '''
  # make sure nomad cluster is up
  single_master = list(masters)[0]

  for i in range(10):
    try:
      r = requests.get("http://%s:4646/v1/status/leader" % single_master)
      if r.status_code == 200:
        break
    except:
      Log.debug(sys.exc_info()[0])
      Log.info("Waiting for cluster to come up... %s" % i)
      time.sleep(1)


  cmd = "%s run %s >> /tmp/apiserver_start.log 2>&1 &" \
        % (get_nomad_path(cl_args), get_apiserver_job_file(cl_args))
  Log.info("Starting Heron API Server on %s" % single_master)

  if not is_self(single_master):
    cmd = ssh_remote_execute(cmd, single_master, cl_args)
  Log.debug(cmd)
  pid = subprocess.Popen(cmd,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

  return_code = pid.wait()
  output = pid.communicate()
  Log.debug("return code: %s output: %s" % (return_code, output))
  if return_code != 0:
    Log.error("Failed to start apiserver on %s with error:\n%s" % (single_master, output[1]))
    sys.exit(-1)

  Log.info("Done starting Heron API Server")

def distribute_package(roles, cl_args):
  '''
  distribute Heron packages to all nodes
  '''

  Log.info("Distributing heron package to nodes (this might take a while)...")
  masters = roles[SET.MASTERS]
  slaves = roles[SET.SLAVES]

  tar_file = tempfile.NamedTemporaryFile(suffix=".tmp").name
  Log.debug("TAR file %s to %s" % (cl_args["heron_dir"], tar_file))
  make_tarfile(tar_file, cl_args["heron_dir"])
  dist_nodes = masters.union(slaves)

  scp_package(tar_file, dist_nodes, cl_args)

def scp_package(package_file, destinations, cl_args):
  '''
  scp and extract package
  '''
  pids = []
  for dest in destinations:
    if is_self(dest):
      continue
    Log.info("Server: %s" % dest)
    file_path = "/tmp/heron.tar.gz"
    dest_file_path = "%s:%s" % (dest, file_path)

    remote_cmd = "rm -rf ~/.heron && mkdir ~/.heron " \
                 "&& tar -xzvf %s -C ~/.heron --strip-components 1" % (file_path)
    cmd = '%s && %s' \
          % (scp_cmd(package_file, dest_file_path, cl_args),
             ssh_remote_execute(remote_cmd, dest, cl_args))
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    pids.append({"pid": pid, "dest": dest})

  errors = []
  for entry in pids:
    pid = entry["pid"]
    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))
    if return_code != 0:
      errors.append("Failed to scp package to %s with error:\n%s" % (entry["dest"], output[1]))

  if errors:
    for error in errors:
      Log.error(error)
    sys.exit(-1)

  Log.info("Done distributing packages")

def make_tarfile(output_filename, source_dir):
  '''
  Tar a directory
  '''
  with tarfile.open(output_filename, "w:gz") as tar:
    tar.add(source_dir, arcname=os.path.basename(source_dir))

def start_master_nodes(masters, cl_args):
  '''
  Start master nodes
  '''

  pids = []
  for master in masters:
    Log.info("Starting master on %s" % master)
    cmd = "%s agent -config %s >> /tmp/nomad_server_log 2>&1 &" \
          % (get_nomad_path(cl_args), get_nomad_master_config_file(cl_args))
    if not is_self(master):
      cmd = ssh_remote_execute(cmd, master, cl_args)
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    pids.append({"pid": pid, "dest": master})

  errors = []
  for entry in pids:
    pid = entry["pid"]
    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))
    if return_code != 0:
      errors.append("Failed to start master on %s with error:\n%s" % (entry["dest"], output[1]))

  if errors:
    for error in errors:
      Log.error(error)
    sys.exit(-1)

  Log.info("Done starting masters")

def start_slave_nodes(slaves, cl_args):
  '''
  Star slave nodes
  '''
  pids = []
  for slave in slaves:
    Log.info("Starting slave on %s" % slave)
    cmd = "%s agent -config %s >> /tmp/nomad_client.log 2>&1 &" \
          % (get_nomad_path(cl_args), get_nomad_slave_config_file(cl_args))
    Log.info("Starting slave on %s" % slave)
    if not is_self(slave):
      cmd = ssh_remote_execute(cmd, slave, cl_args)
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    pids.append({"pid": pid, "dest": slave})

  errors = []
  for entry in pids:
    pid = entry["pid"]
    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))
    if return_code != 0:
      errors.append("Failed to start slave on %s with error:\n%s" % (entry["dest"], output[1]))

  if errors:
    for error in errors:
      Log.error(error)
    sys.exit(-1)

  Log.info("Done starting slaves")


def read_and_parse_roles(cl_args):
  '''
  read config files to get roles
  '''
  roles = dict()
  roles[SET.ZOOKEEPERS] = set(read_file(get_role_definition_file(SET.ZOOKEEPERS, cl_args)))
  roles[SET.MASTERS] = set(read_file(get_role_definition_file(SET.MASTERS, cl_args)))
  roles[SET.SLAVES] = set(read_file(get_role_definition_file(SET.SLAVES, cl_args)))
  return roles

def read_file(file_path):
  '''
  read file
  '''
  lines = []
  with open(file_path, "r") as tf:
    lines = [line.strip("\n") for line in tf.readlines() if not line.startswith("#")]
    # filter empty lines
    lines = [line for line in lines if line]
  return lines

def call_editor(file_path):
  '''
  call editor
  '''
  EDITOR = os.environ.get('EDITOR', 'vim')
  with open(file_path, 'r+') as tf:
    call([EDITOR, tf.name])

def get_role_definition_file(role, cl_args):
  '''
  get the location of role files
  '''
  if role == SET.ZOOKEEPERS:
    return "%s/standalone/roles/zookeeper_servers.txt" % cl_args["config_path"]
  if role == SET.MASTERS:
    return "%s/standalone/roles/master_servers.txt" % cl_args["config_path"]
  if role == SET.SLAVES:
    return "%s/standalone/roles/slave_servers.txt" % cl_args["config_path"]

def ssh_remote_execute(cmd, host, cl_args):
  '''
  get ssh remote execute command
  '''
  ssh = 'ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null %s "%s"' % (host, cmd)
  return ssh

def scp_cmd(src, dest, cl_args):
  '''
  get scp command
  '''
  scp = 'scp -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null %s %s ' % (src, dest)
  return scp

def get_nomad_path(cl_args):
  '''
  get path to nomad binary
  '''
  return "~/.heron/bin/heron-nomad"

def get_nomad_master_config_file(cl_args):
  '''
  get path to nomad master config file
  '''
  return "~/.heron/conf/standalone/resources/master.hcl"

def get_nomad_slave_config_file(cl_args):
  '''
  get path to nomad slave config file
  '''
  return "~/.heron/conf/standalone/resources/slave.hcl"

def get_apiserver_job_file(cl_args):
  '''
  get path to api server job file
  '''
  return "~/.heron/conf/standalone/resources/apiserver.hcl"

def get_remote_home(host, cl_args):
  '''
  get home directory of remote host
  '''
  cmd = "echo ~"
  if not is_self(host):
    cmd = ssh_remote_execute(cmd, host, cl_args)
  pid = subprocess.Popen(cmd,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
  return_code = pid.wait()
  output = pid.communicate()

  if return_code != 0:
    Log.error("Failed to get home path for remote host %s with output:\n%s" % (host, output))
    sys.exit(-1)
  return output[0].strip("\n")

def get_self_ip():
  '''
  get IP address of self
  '''
  return socket.gethostbyname(socket.gethostname())

def get_self_hostname():
  '''
  get hostname of self
  '''
  return socket.gethostname()

def get_hostname(ip_addr, cl_args):
  '''
  get host name of remote host
  '''
  if is_self(ip_addr):
    return get_self_hostname()
  cmd = "hostname"
  ssh_cmd = ssh_remote_execute(cmd, ip_addr, cl_args)
  pid = subprocess.Popen(ssh_cmd,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
  return_code = pid.wait()
  output = pid.communicate()

  if return_code != 0:
    Log.error("Failed to get hostname for remote host %s with output:\n%s" % (ip_addr, output))
    sys.exit(-1)
  return output[0].strip("\n")

def check_sure(cl_args, prompt):
  yes = raw_input("%s" % prompt + ' (yes/no): ')
  if yes == "y" or yes == "yes":
    return True
  elif yes == "n" or yes == "no":
    return False
  else:
    print 'Invalid input.  Please input "yes" or "no"'

def get_jobs(cl_args, nomad_addr):
  r = requests.get("http://%s:4646/v1/jobs" % nomad_addr)
  if r.status_code != 200:
    Log.error("Failed to get list of jobs")
    Log.debug("Response: %s" % r)
    sys.exit(-1)
  return r.json()

def delete_job(cl_args, job_id, nomad_addr):
  r = requests.delete("http://%s:4646/v1/job/%s" % (nomad_addr, job_id), data={'purge':'true'})
  if r.status_code != 200:
    Log.error("Failed to delete job %s" % job_id)
    Log.debug("Response: %s" % r)
    sys.exit(-1)

def is_self(addr):
  '''
  check if this host is this addr
  '''
  ips = []
  for i in netifaces.interfaces():
    entry = netifaces.ifaddresses(i)
    if netifaces.AF_INET in entry:
      for ipv4 in entry[netifaces.AF_INET]:
        if "addr" in ipv4:
          ips.append(ipv4["addr"])
  return addr in ips or addr == get_self_hostname()
