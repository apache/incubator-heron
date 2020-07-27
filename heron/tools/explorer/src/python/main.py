#!/usr/bin/env python3
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

''' main.py '''
import logging
import os
import sys

import heron.common.src.python.utils.log as log
import heron.tools.common.src.python.access.tracker_access as tracker_access
import heron.tools.common.src.python.utils.config as config
import heron.tools.explorer.src.python.logicalplan as logicalplan
import heron.tools.explorer.src.python.physicalplan as physicalplan
import heron.tools.explorer.src.python.topologies as topologies

import click

from tornado.options import define

Log = log.Log

DEFAULT_TRACKER_URL = "http://127.0.0.1:8888"

try:
  click_extra = {"max_content_width": os.get_terminal_size().columns}
except Exception:
  click_extra = {}

def show_version(_, __, value):
  if value:
    config.print_build_info()
    sys.exit(0)

@click.group(context_settings=click_extra)
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=show_version,
)
@click.option("--verbose", is_flag=True)
def cli(verbose: bool):
  log.configure(logging.INFO if verbose else logging.DEBUG)

@cli.command("clusters")
@click.option("--tracker-url", default=DEFAULT_TRACKER_URL)
def cli_clusters(tracker_url: str):
  define("tracker_url", tracker_url)
  try:
    clusters = tracker_access.get_clusters()
  except:
    Log.error("Fail to connect to tracker")
    sys.exit(1)
  print("Available clusters:")
  for cluster in clusters:
    print(f"  {cluster}")

@cli.command("topologies")
@click.option("--tracker-url", default=DEFAULT_TRACKER_URL)
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
def cli_topologies(tracker_url: str, cre: str):
  define("tracker_url", tracker_url)
  topologies.run(
      cre=cre,
  )

@cli.command()
@click.option("--config-path", default=config.get_heron_conf_dir())
@click.option("--tracker-url", default=DEFAULT_TRACKER_URL)
@click.option("--component-type", type=click.Choice(["all", "spouts", "bolts"]), default="all")
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
@click.argument("topology")
def logical_plan(
    config_path: str,
    cre: str,
    topology: str,
    component_type: str,
    tracker_url: str,
) -> None:
  define("tracker_url", tracker_url)
  cluster = config.get_heron_cluster(cre)
  cluster_config_path = config.get_heron_cluster_conf_dir(cluster, config_path)
  cluster, role, environment = config.parse_cluster_role_env(cre, cluster_config_path)

  logicalplan.run(
      component_type=component_type,
      cluster=cluster,
      role=role,
      environment=environment,
      topology=topology,
  )

@cli.group()
def physical_plan():
  pass

@physical_plan.command()
@click.option("--config-path", default=config.get_heron_conf_dir())
@click.option("--tracker-url", default=DEFAULT_TRACKER_URL)
@click.option("--component")
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
@click.argument("topology")
def metrics(
    config_path: str,
    cre: str,
    tracker_url: str,
    topology: str,
    component: str,
) -> None:
  define("tracker_url", tracker_url)
  cluster = config.get_heron_cluster(cre)
  cluster_config_path = config.get_heron_cluster_conf_dir(cluster, config_path)
  cluster, role, environment = config.parse_cluster_role_env(cre, cluster_config_path)

  physicalplan.run_metrics(
      cluster=cluster,
      role=role,
      environment=environment,
      component=component,
      topology=topology,
  )

def validate_container_id(_, __, value):
  if value is None:
    return None
  if value <= 0:
    raise click.BadParameter("container id must be greather than zero")
  return value - 1

@physical_plan.command()
@click.option("--config-path", default=config.get_heron_conf_dir())
@click.option("--tracker-url", default=DEFAULT_TRACKER_URL)
@click.option("--id", "container_id", type=int, help="container id", callback=validate_container_id)
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
@click.argument("topology")
def containers(
    config_path: str,
    cre: str,
    tracker_url: str,
    topology: str,
    container_id: int,
) -> None:
  define("tracker_url", tracker_url)
  cluster = config.get_heron_cluster(cre)
  cluster_config_path = config.get_heron_cluster_conf_dir(cluster, config_path)
  cluster, role, environment = config.parse_cluster_role_env(cre, cluster_config_path)

  physicalplan.run_containers(
      cluster=cluster,
      role=role,
      environment=environment,
      container_id=container_id,
      topology=topology,
  )

if __name__ == "__main__":
  cli() # pylint: disable=no-value-for-parameter
