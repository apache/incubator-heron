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

import click
import requests

from heron.common.src.python.utils import log
from heron.tools.common.src.python.clients import tracker
from heron.tools.common.src.python.utils import config
from heron.tools.explorer.src.python import logicalplan
from heron.tools.explorer.src.python import physicalplan
from heron.tools.explorer.src.python import topologies

Log = log.Log

DEFAULT_TRACKER_URL = "http://127.0.0.1:8888"

try:
  click_extra = {"max_content_width": os.get_terminal_size().columns}
except Exception:
  click_extra = {}


def config_path_option():
  return click.option(
      "--config-path",
      default=config.get_heron_conf_dir(),
      show_default=True,
      help="Path to heron's config clusters config directory"
  )

def tracker_url_option():
  return click.option(
      "--tracker-url",
      default=DEFAULT_TRACKER_URL,
      show_default=True,
      help="URL to a heron-tracker instance"
  )

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
@click.option("-v", "--verbose", count=True)
def cli(verbose: int):
  levels = {
      0: logging.WARNING,
      1: logging.INFO,
      2: logging.DEBUG,
  }
  log.configure(levels.get(verbose, logging.DEBUG))

@cli.command("clusters")
@tracker_url_option()
def cli_clusters(tracker_url: str):
  tracker.tracker_url = tracker_url
  try:
    clusters = tracker.get_clusters()
  except requests.ConnectionError as e:
    Log.error(f"Fail to connect to tracker: {e}")
    sys.exit(1)
  print("Available clusters:")
  for cluster in clusters:
    print(f"  {cluster}")

@cli.command("topologies")
@tracker_url_option()
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
def cli_topologies(tracker_url: str, cre: str):
  """Show the topologies under the given CLUSTER[/ROLE[/ENV]]."""
  tracker.tracker_url = tracker_url
  topologies.run(
      cre=cre,
  )

@cli.command()
@config_path_option()
@tracker_url_option()
@click.option(
    "--component-type",
    type=click.Choice(["all", "spouts", "bolts"]),
    default="all",
    show_default=True,
)
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
@click.argument("topology")
def logical_plan(
    config_path: str,
    cre: str,
    topology: str,
    component_type: str,
    tracker_url: str,
) -> None:
  """Show logical plan information for the given topology."""
  tracker.tracker_url = tracker_url
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
@config_path_option()
@tracker_url_option()
@click.option("--component", help="name of component to limit metrics to")
@click.argument("cre", metavar="CLUSTER[/ROLE[/ENV]]")
@click.argument("topology")
def metrics(
    config_path: str,
    cre: str,
    tracker_url: str,
    topology: str,
    component: str,
) -> None:
  tracker.tracker_url = tracker_url
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
@config_path_option()
@tracker_url_option()
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
  tracker.tracker_url = tracker_url
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
