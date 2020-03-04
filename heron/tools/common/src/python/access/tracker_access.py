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

'''tracker_access.py: util functions for heron explorer and tracker'''
import traceback
import tornado.gen
import tornado.ioloop

from heron.tools.common.src.python.access import heron_api as API
from heron.common.src.python.utils.log import Log

def _all_metric_queries():
  queries_normal = ['complete-latency',
                    'execute-latency',
                    'process-latency',
                    'jvm-uptime-secs',
                    'jvm-process-cpu-load',
                    'jvm-memory-used-mb']
  queries = ['__%s' % m for m in queries_normal]
  count_queries_normal = ['emit-count', 'execute-count', 'ack-count', 'fail-count']
  count_queries = ['__%s/default' % m for m in count_queries_normal]
  return queries, queries_normal, count_queries, count_queries_normal


def metric_queries():
  """all metric queries"""
  qs = _all_metric_queries()
  return qs[0] + qs[2]


def queries_map():
  """map from query parameter to query name"""
  qs = _all_metric_queries()
  return dict(list(zip(qs[0], qs[1])) + list(zip(qs[2], qs[3])))


def get_clusters():
  """Synced API call to get all cluster names"""
  instance = tornado.ioloop.IOLoop.instance()
  # pylint: disable=unnecessary-lambda
  try:
    return instance.run_sync(lambda: API.get_clusters())
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_logical_plan(cluster, env, topology, role):
  """Synced API call to get logical plans"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_logical_plan(cluster, env, topology, role))
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_topology_info(*args):
  """Synced API call to get topology information"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_topology_info(*args))
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_topology_metrics(*args):
  """Synced API call to get topology metrics"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_comp_metrics(*args))
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_component_metrics(component, cluster, env, topology, role):
  """Synced API call to get component metrics"""
  all_queries = metric_queries()
  try:
    result = get_topology_metrics(cluster, env, topology, component, [],
                                  all_queries, [0, -1], role)
    return result["metrics"]
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_cluster_topologies(cluster):
  """Synced API call to get topologies under a cluster"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_topologies(cluster))
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_cluster_role_topologies(cluster, role):
  """Synced API call to get topologies under a cluster submitted by a role"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_role_topologies(cluster, role))
  except Exception:
    Log.debug(traceback.format_exc())
    raise


def get_cluster_role_env_topologies(cluster, role, env):
  """Synced API call to get topologies under a cluster submitted by a role under env"""
  instance = tornado.ioloop.IOLoop.instance()
  try:
    return instance.run_sync(lambda: API.get_cluster_role_env_topologies(cluster, role, env))
  except Exception:
    Log.debug(traceback.format_exc())
    raise
