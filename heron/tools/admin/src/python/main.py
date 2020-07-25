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

''' standalone.py '''
from collections import OrderedDict
from subprocess import call

import json
import logging
import os
import socket
import subprocess
import sys
import tarfile
import tempfile
import time

from heron.common.src.python.utils import log
import heron.tools.common.src.python.utils.config as config

import requests
import netifaces
import yaml
import click

Log = log.Log
# pylint: disable=too-many-branches

class Role:
  ZOOKEEPERS = "zookeepers"
  MASTERS = "masters"
  SLAVES = "slaves"
  CLUSTER = "cluster"

class Get:
  SERVICE_URL = "service-url"
  HERON_TRACKER_URL = "heron-tracker-url"
  HERON_UI_URL = "heron-ui-url"

################################################################################
@click.group()
@click.option("--verbose", is_flag=True)
def cli(verbose: bool):
  if not config.check_release_file_exists():
    sys.exit(1)
  log.configure(logging.DEBUG if verbose else logging.INFO)

@cli.group()
def standalone():
  pass

@standalone.group("cluster", help="Start a standalone Heron cluster")
def standalone_cluster():
  pass

@standalone_cluster.command("stop")
@click.option("--config-path", default=config.get_heron_conf_dir(), envvar="HERON_CONFIG")
@click.confirmation_option(prompt="Are you sure you want to stop the cluster?"
                                  " This will terminate everything running in "
                                  "the cluster and remove any scheduler state.")
def standalone_cluster_stop(config_path: str):
  """Stop the stanalone cluster."""
  inventory_file_path = get_inventory_file(config_path)
  stop_cluster(inventory_file_path)


@standalone_cluster.command("start")
@click.option("--config-path", default=config.get_heron_conf_dir(), envvar="HERON_CONFIG")
@click.option("--heron-dir", default=config.get_heron_dir(), envvar="HERON_DIR")
def standalone_cluster_start(config_path: str, heron_dir: str):
  """Start the standalone cluster."""
  inventory_file_path = get_inventory_file(config_path)
  start_cluster(config_path, inventory_file_path, heron_dir)

@standalone.command("get")
@click.option("--config-path", default=config.get_heron_conf_dir(), envvar="HERON_CONFIG")
@click.argument("variable",
                type=click.Choice([Get.SERVICE_URL, Get.HERON_TRACKER_URL, Get.HERON_UI_URL]))
def standalone_get(config_path: str, variable: str):
  """Display the value of specific cluster config values."""
  inventory_file_path = get_inventory_file(config_path)
  if variable == Get.SERVICE_URL:
    print(get_service_url(inventory_file_path))
  elif variable == Get.HERON_UI_URL:
    print(get_heron_ui_url(inventory_file_path))
  elif variable == Get.HERON_TRACKER_URL:
    print(get_heron_tracker_url(inventory_file_path))

@standalone.command("set")
@click.option("--config-path", default=config.get_heron_conf_dir(), envvar="HERON_CONFIG")
@click.option("--edit/--no-edit", default=True, help="open inventory for editing first")
def standalone_set(config_path: str, edit: bool) -> None:
  """
  Open an editor for the cluster's inventory.yaml, then apply the changes on exit.

  """
  inventory_file_path = get_inventory_file(config_path)
  if edit:
    call_editor(inventory_file_path)
  update_config_files(config_path, inventory_file_path)

@standalone.command("template")
@click.option("--config-path", default=config.get_heron_conf_dir(), envvar="HERON_CONFIG")
def standalone_template(config_path: str):
  """
  This should probably be replace with `heron-admin standalone set --no-edit`
  """
  standalone_set(config_path, edit=False)

@standalone.command("info")
@click.option("--config-path", default=config.get_heron_conf_dir(), envvar="HERON_CONFIG")
def standalone_info(config_path: str):
  """
  Get general information about the standalone cluster

  """
  inventory_file_path = get_inventory_file(config_path)
  print_cluster_info(inventory_file_path)

################################################################################

def update_config_files(config_path, inventory_file_path):
  Log.info("Updating config files...")
  roles = read_and_parse_roles(inventory_file_path)
  Log.debug("roles: %s" % roles)
  masters = list(roles[Role.MASTERS])
  zookeepers = list(roles[Role.ZOOKEEPERS])

  template_slave_hcl(config_path, masters)
  template_scheduler_yaml(config_path, masters)
  template_uploader_yaml(config_path, masters)
  template_apiserver_hcl(config_path, masters, zookeepers)
  template_statemgr_yaml(config_path, zookeepers)
  template_heron_tools_hcl(config_path, masters, zookeepers)

##################### Templating functions ######################################

def template_slave_hcl(config_path: str, masters):
  '''
  Template slave config file
  '''
  slave_config_template = f"{config_path}/standalone/templates/slave.template.hcl"
  slave_config_actual = f"{config_path}/standalone/resources/slave.hcl"
  masters_in_quotes = ['"%s"' % master for master in masters]
  template_file(slave_config_template, slave_config_actual,
                {"<nomad_masters:master_port>": ", ".join(masters_in_quotes)})

def template_scheduler_yaml(config_path: str, masters):
  '''
  Template scheduler.yaml
  '''
  single_master = masters[0]
  scheduler_config_actual = f"{config_path}/standalone/scheduler.yaml"

  scheduler_config_template = f"{config_path}/standalone/templates/scheduler.template.yaml"
  template_file(scheduler_config_template, scheduler_config_actual,
                {"<scheduler_uri>": "http://%s:4646" % single_master})

def template_uploader_yaml(config_path: str, masters):
  '''
  Tempate uploader.yaml
  '''
  single_master = masters[0]
  uploader_config_template = f"{config_path}/standalone/templates/uploader.template.yaml"
  uploader_config_actual = f"{config_path}/standalone/uploader.yaml"

  template_file(uploader_config_template, uploader_config_actual,
                {"<http_uploader_uri>": "http://%s:9000/api/v1/file/upload" % single_master})

def template_apiserver_hcl(config_path: str, masters, zookeepers):
  """
  template apiserver.hcl
  """
  single_master = masters[0]
  apiserver_config_template = f"{config_path}/standalone/templates/apiserver.template.hcl"
  apiserver_config_actual = f"{config_path}/standalone/resources/apiserver.hcl"

  replacements = {
      "<heron_apiserver_hostname>": '"%s"' % get_hostname(single_master),
      "<heron_apiserver_executable>": '"%s/heron-apiserver"'
                                      % config.get_heron_bin_dir()
                                      if is_self(single_master)
                                      else '"%s/.heron/bin/heron-apiserver"'
                                      % get_remote_home(single_master),
      "<zookeeper_host:zookeeper_port>": ",".join(
          ['%s' % zk if ":" in zk else '%s:2181' % zk for zk in zookeepers]),
      "<scheduler_uri>": "http://%s:4646" % single_master
  }

  template_file(apiserver_config_template, apiserver_config_actual, replacements)


def template_statemgr_yaml(config_path: str, zookeepers):
  '''
  Template statemgr.yaml
  '''
  statemgr_config_file_template = f"{config_path}/standalone/templates/statemgr.template.yaml"
  statemgr_config_file_actual = f"{config_path}/standalone/statemgr.yaml"

  template_file(statemgr_config_file_template, statemgr_config_file_actual,
                {"<zookeeper_host:zookeeper_port>": ",".join(
                    ['"%s"' % zk if ":" in zk else '"%s:2181"' % zk for zk in zookeepers])})

def template_heron_tools_hcl(config_path: str, masters, zookeepers):
  '''
  template heron tools
  '''
  heron_tools_hcl_template = f"{config_path}/standalone/templates/heron_tools.template.hcl"
  heron_tools_hcl_actual = f"{config_path}/standalone/resources/heron_tools.hcl"

  single_master = masters[0]
  template_file(heron_tools_hcl_template, heron_tools_hcl_actual,
                {
                    "<zookeeper_host:zookeeper_port>": ",".join(
                        ['%s' % zk if ":" in zk else '%s:2181' % zk for zk in zookeepers]),
                    "<heron_tracker_executable>": '"%s/heron-tracker"' % config.get_heron_bin_dir(),
                    "<heron_tools_hostname>": '"%s"' % get_hostname(single_master),
                    "<heron_ui_executable>": '"%s/heron-ui"' % config.get_heron_bin_dir()
                })

def template_file(src, dest, replacements_dict):
  Log.debug("Templating %s - > %s with %s" % (src, dest, replacements_dict))

  file_contents = ""
  with open(src, 'r') as tf:
    file_contents = tf.read()
    for key, value in replacements_dict.items():
      file_contents = file_contents.replace(key, value)

  if not file_contents:
    Log.error("File contents after templating is empty")
    sys.exit(-1)

  with open(dest, 'w') as tf:
    tf.write(file_contents)
    tf.truncate()

################################################################################

def get_service_url(inventory_file_path: str) -> str:
  '''
  get service url for standalone cluster
  '''
  roles = read_and_parse_roles(inventory_file_path)
  return "http://%s:9000" % list(roles[Role.MASTERS])[0]

def get_heron_tracker_url(inventory_file_path: str) -> str:
  '''
  get service url for standalone cluster
  '''
  roles = read_and_parse_roles(inventory_file_path)
  return "http://%s:8888" % list(roles[Role.MASTERS])[0]

def get_heron_ui_url(inventory_file_path: str) -> None:
  '''
  get service url for standalone cluster
  '''
  roles = read_and_parse_roles(inventory_file_path)
  return "http://%s:8889" % list(roles[Role.MASTERS])[0]

def print_cluster_info(inventory_file_path: str):
  '''
  get cluster info for standalone cluster
  '''
  parsed_roles = read_and_parse_roles(inventory_file_path)
  masters = list(parsed_roles[Role.MASTERS])
  slaves = list(parsed_roles[Role.SLAVES])
  zookeepers = list(parsed_roles[Role.ZOOKEEPERS])
  cluster = list(parsed_roles[Role.CLUSTER])

  # OrderedDicts are used here so that the key order can be
  # specified directly
  info = OrderedDict()
  info['numNodes'] = len(cluster)
  info['nodes'] = cluster
  roles = OrderedDict()
  roles['masters'] = masters
  roles['slaves'] = slaves
  roles['zookeepers'] = zookeepers
  urls = OrderedDict()
  urls['serviceUrl'] = get_service_url(inventory_file_path)
  urls['heronUi'] = get_heron_ui_url(inventory_file_path)
  urls['heronTracker'] = get_heron_tracker_url(inventory_file_path)
  info['roles'] = roles
  info['urls'] = urls

  print(json.dumps(info, indent=2))

def stop_cluster(inventory_file_path: str):
  '''
  teardown the cluster
  '''
  Log.info("Terminating cluster...")

  roles = read_and_parse_roles(inventory_file_path)
  masters = roles[Role.MASTERS]
  slaves = roles[Role.SLAVES]
  dist_nodes = masters.union(slaves)

  # stop all jobs
  if masters:
    try:
      single_master = list(masters)[0]
      jobs = get_jobs(single_master)
      for job in jobs:
        job_id = job["ID"]
        Log.info("Terminating job %s" % job_id)
        delete_job(job_id, single_master)
    except:
      Log.debug("Error stopping jobs")
      Log.debug(sys.exc_info()[0])

  for node in dist_nodes:
    Log.info("Terminating processes on %s" % node)
    if not is_self(node):
      cmd = "ps aux | grep [n]omad | awk '{print \\$2}' " \
            "| xargs kill"
      cmd = ssh_remote_execute(cmd, node)
    else:
      cmd = "ps aux | grep [n]omad | awk '{print $2}' " \
            "| xargs kill"
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           universal_newlines=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))

    Log.info("Cleaning up directories on %s" % node)
    cmd = "rm -rf /tmp/slave ; rm -rf /tmp/master"
    if not is_self(node):
      cmd = ssh_remote_execute(cmd, node)
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           universal_newlines=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    return_code = pid.wait()
    output = pid.communicate()
    Log.debug("return code: %s output: %s" % (return_code, output))

def start_cluster(config_path: str, inventory_file_path: str, heron_dir: str):
  '''
  Start a Heron standalone cluster
  '''
  roles = read_and_parse_roles(inventory_file_path)
  masters = roles[Role.MASTERS]
  slaves = roles[Role.SLAVES]
  zookeepers = roles[Role.ZOOKEEPERS]
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
  update_config_files(config_path, inventory_file_path)

  dist_nodes = list(masters.union(slaves))
  # if just local deployment
  if not (len(dist_nodes) == 1 and is_self(dist_nodes[0])):
    distribute_package(roles, heron_dir)
  start_master_nodes(masters)
  start_slave_nodes(slaves)
  start_api_server(masters)
  start_heron_tools(masters)
  Log.info("Heron standalone cluster complete!")

def start_api_server(masters):
  '''
  Start the Heron API server
  '''
  # make sure nomad cluster is up
  single_master = list(masters)[0]
  wait_for_master_to_start(single_master)

  cmd = "%s run %s >> /tmp/apiserver_start.log 2>&1 &" \
        % (get_nomad_path(), get_apiserver_job_file())
  Log.info("Starting Heron API Server on %s" % single_master)

  if not is_self(single_master):
    cmd = ssh_remote_execute(cmd, single_master)
  Log.debug(cmd)
  pid = subprocess.Popen(cmd,
                         shell=True,
                         universal_newlines=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

  return_code = pid.wait()
  output = pid.communicate()
  Log.debug("return code: %s output: %s" % (return_code, output))
  if return_code != 0:
    Log.error("Failed to start API server on %s with error:\n%s" % (single_master, output[1]))
    sys.exit(-1)

  wait_for_job_to_start(single_master, "apiserver")
  Log.info("Done starting Heron API Server")

def start_heron_tools(masters):
  '''
  Start Heron tracker and UI
  '''
  single_master = list(masters)[0]
  wait_for_master_to_start(single_master)

  cmd = "%s run %s >> /tmp/heron_tools_start.log 2>&1 &" \
        % (get_nomad_path(), get_heron_tools_job_file())
  Log.info("Starting Heron Tools on %s" % single_master)

  if not is_self(single_master):
    cmd = ssh_remote_execute(cmd, single_master)
  Log.debug(cmd)
  pid = subprocess.Popen(cmd,
                         shell=True,
                         universal_newlines=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

  return_code = pid.wait()
  output = pid.communicate()
  Log.debug("return code: %s output: %s" % (return_code, output))
  if return_code != 0:
    Log.error("Failed to start Heron Tools on %s with error:\n%s" % (single_master, output[1]))
    sys.exit(-1)

  wait_for_job_to_start(single_master, "heron-tools")
  Log.info("Done starting Heron Tools")

def distribute_package(roles, heron_dir: str):
  '''
  distribute Heron packages to all nodes
  '''
  Log.info("Distributing heron package to nodes (this might take a while)...")
  masters = roles[Role.MASTERS]
  slaves = roles[Role.SLAVES]

  tar_file = tempfile.NamedTemporaryFile(suffix=".tmp").name
  Log.debug("TAR file %s to %s" % (heron_dir, tar_file))
  make_tarfile(tar_file, heron_dir)
  dist_nodes = masters.union(slaves)

  scp_package(tar_file, dist_nodes)

def wait_for_master_to_start(single_master):
  '''
  Wait for a nomad master to start
  '''
  i = 0
  while True:
    try:
      r = requests.get("http://%s:4646/v1/status/leader" % single_master)
      if r.status_code == 200:
        break
    except:
      Log.debug(sys.exc_info()[0])
      Log.info("Waiting for cluster to come up... %s" % i)
      time.sleep(1)
      if i > 10:
        Log.error("Failed to start Nomad Cluster!")
        sys.exit(-1)
    i = i + 1

def wait_for_job_to_start(single_master, job):
  '''
  Wait for a Nomad job to start
  '''
  i = 0
  while True:
    try:
      r = requests.get("http://%s:4646/v1/job/%s" % (single_master, job))
      if r.status_code == 200 and r.json()["Status"] == "running":
        break
      raise RuntimeError()
    except:
      Log.debug(sys.exc_info()[0])
      Log.info("Waiting for %s to come up... %s" % (job, i))
      time.sleep(1)
      if i > 20:
        Log.error("Failed to start Nomad Cluster!")
        sys.exit(-1)
    i = i + 1

def scp_package(package_file, destinations):
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
          % (scp_cmd(package_file, dest_file_path),
             ssh_remote_execute(remote_cmd, dest))
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           universal_newlines=True,
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

def start_master_nodes(masters):
  '''
  Start master nodes
  '''
  pids = []
  for master in masters:
    Log.info("Starting master on %s" % master)
    cmd = "%s agent -config %s >> /tmp/nomad_server_log 2>&1 &" \
          % (get_nomad_path(), get_nomad_master_config_file())
    if not is_self(master):
      cmd = ssh_remote_execute(cmd, master)
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           universal_newlines=True,
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

def start_slave_nodes(slaves):
  '''
  Star slave nodes
  '''
  pids = []
  for slave in slaves:
    Log.info("Starting slave on %s" % slave)
    cmd = "%s agent -config %s >> /tmp/nomad_client.log 2>&1 &" \
          % (get_nomad_path(), get_nomad_slave_config_file())
    if not is_self(slave):
      cmd = ssh_remote_execute(cmd, slave)
    Log.debug(cmd)
    pid = subprocess.Popen(cmd,
                           shell=True,
                           universal_newlines=True,
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


def read_and_parse_roles(inventory_file_path):
  '''
  read config files to get roles
  '''
  roles = {}

  with open(inventory_file_path, 'r') as stream:
    try:
      roles = yaml.load(stream)
    except yaml.YAMLError as exc:
      Log.error("Error parsing inventory file: %s" % exc)
      sys.exit(-1)

  if Role.ZOOKEEPERS not in roles or not roles[Role.ZOOKEEPERS]:
    Log.error("Zookeeper servers node defined!")
    sys.exit(-1)

  if Role.CLUSTER not in roles or not roles[Role.CLUSTER]:
    Log.error("Heron cluster nodes defined!")
    sys.exit(-1)

  # Set roles
  roles[Role.MASTERS] = set([roles[Role.CLUSTER][0]])
  roles[Role.SLAVES] = set(roles[Role.CLUSTER])
  roles[Role.ZOOKEEPERS] = set(roles[Role.ZOOKEEPERS])
  roles[Role.CLUSTER] = set(roles[Role.CLUSTER])

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
  Open an editor for the given file.
  '''
  for env in ('VISUAL', 'EDITOR'):
    editor = os.environ.get(env)
    if editor:
      break
  else:
    editor = 'vi'
  if call([editor, file_path]):
    raise RuntimeError("editor returned non-0 exit code")

def get_inventory_file(config_path: str):
  '''
  get the location of inventory file
  '''
  return f"{config_path}/standalone/inventory.yaml"

def ssh_remote_execute(cmd, host):
  '''
  get ssh remote execute command
  '''
  ssh = 'ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null %s "%s"' % (host, cmd)
  return ssh

def scp_cmd(src, dest):
  '''
  get scp command
  '''
  scp = 'scp -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null %s %s ' % (src, dest)
  return scp

def get_nomad_path():
  '''
  get path to nomad binary
  '''

  return "%s/nomad" % config.get_heron_bin_dir()

def get_nomad_master_config_file():
  '''
  get path to nomad master config file
  '''
  return "%s/standalone/resources/master.hcl" % config.get_heron_conf_dir()

def get_nomad_slave_config_file():
  '''
  get path to nomad slave config file
  '''
  return "%s/standalone/resources/slave.hcl" % config.get_heron_conf_dir()

def get_apiserver_job_file():
  '''
  get path to API server job file
  '''
  return "%s/standalone/resources/apiserver.hcl" % config.get_heron_conf_dir()

def get_heron_tools_job_file():
  '''
  get path to API server job file
  '''
  return "%s/standalone/resources/heron_tools.hcl" % config.get_heron_conf_dir()

def get_remote_home(host):
  '''
  get home directory of remote host
  '''
  cmd = "echo ~"
  if not is_self(host):
    cmd = ssh_remote_execute(cmd, host)
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

def get_hostname(ip_addr):
  '''
  get host name of remote host
  '''
  if is_self(ip_addr):
    return get_self_hostname()
  cmd = "hostname"
  ssh_cmd = ssh_remote_execute(cmd, ip_addr)
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

def get_jobs(nomad_addr):
  r = requests.get("http://%s:4646/v1/jobs" % nomad_addr)
  if r.status_code != 200:
    Log.error("Failed to get list of jobs")
    Log.debug("Response: %s" % r)
    sys.exit(-1)
  return r.json()

def delete_job(job_id, nomad_addr):
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

if __name__ == "__main__":
  cli() # pylint: disable=no-value-for-parameter
