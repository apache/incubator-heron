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

''' heron.py '''

import logging

import tornado.httpclient
import tornado.gen
from tornado.options import options
from .fetch import fetch_url_as_json
from .query import QueryHandler

# pylint: disable=bad-whitespace
CLUSTER_URL_FMT             = "%s/clusters"
TOPOLOGIES_URL_FMT          = "%s/topologies"
TOPOLOGIES_STATS_URL_FMT    = "%s/states"             % TOPOLOGIES_URL_FMT
EXECUTION_STATE_URL_FMT     = "%s/executionstate"     % TOPOLOGIES_URL_FMT
LOGICALPLAN_URL_FMT         = "%s/logicalplan"        % TOPOLOGIES_URL_FMT
PHYSICALPLAN_URL_FMT        = "%s/physicalplan"       % TOPOLOGIES_URL_FMT
PACKINGPLAN_URL_FMT         = "%s/packingplan"        % TOPOLOGIES_URL_FMT
SCHEDULER_LOCATION_URL_FMT  = "%s/schedulerlocation"  % TOPOLOGIES_URL_FMT

METRICS_URL_FMT             = "%s/metrics"            % TOPOLOGIES_URL_FMT
METRICS_QUERY_URL_FMT       = "%s/metricsquery"       % TOPOLOGIES_URL_FMT
METRICS_TIMELINE_URL_FMT    = "%s/metricstimeline"    % TOPOLOGIES_URL_FMT

EXCEPTIONS_URL_FMT          = "%s/exceptions"         % TOPOLOGIES_URL_FMT
EXCEPTION_SUMMARY_URL_FMT   = "%s/exceptionsummary"   % TOPOLOGIES_URL_FMT

INFO_URL_FMT                = "%s/info"               % TOPOLOGIES_URL_FMT
PID_URL_FMT                 = "%s/pid"                % TOPOLOGIES_URL_FMT
JSTACK_URL_FMT              = "%s/jstack"             % TOPOLOGIES_URL_FMT
JMAP_URL_FMT                = "%s/jmap"               % TOPOLOGIES_URL_FMT
HISTOGRAM_URL_FMT           = "%s/histo"              % TOPOLOGIES_URL_FMT

FILE_DATA_URL_FMT           = "%s/containerfiledata"  % TOPOLOGIES_URL_FMT
FILE_DOWNLOAD_URL_FMT       = "%s/containerfiledownload"  % TOPOLOGIES_URL_FMT
FILESTATS_URL_FMT           = "%s/containerfilestats" % TOPOLOGIES_URL_FMT

capacity = "DIVIDE(" \
           "  DEFAULT(0," \
           "    MULTIPLY(" \
           "      TS({0},{1},__execute-count/default)," \
           "      TS({0},{1},__execute-latency/default)" \
           "    )" \
           "  )," \
           "  60000000000" \
           ")"

failures = "DEFAULT(0," \
           "  DIVIDE(" \
           "    TS({0},{1},__fail-count/default)," \
           "    SUM(" \
           "      DEFAULT(1, TS({0},{1},__execute-count/default))," \
           "      DEFAULT(0, TS({0},{1},__fail-count/default))" \
           "    )" \
           "  )" \
           ")"

cpu = "DEFAULT(0, TS({0},{1},__jvm-process-cpu-load))"

memory = "DIVIDE(" \
         "  DEFAULT(0, TS({0},{1},__jvm-memory-used-mb))," \
         "  DEFAULT(1, TS({0},{1},__jvm-memory-mb-total))" \
         ")"

gc = "RATE(TS({0},{1},__jvm-gc-collection-time-ms))"

backpressure = "DEFAULT(0, TS(__stmgr__,*,__time_spent_back_pressure_by_compid/{0}))"

queries = dict(
    cpu=cpu,
    capacity=capacity,
    failures=failures,
    memory=memory,
    gc=gc,
    backpressure=backpressure
)


def get_tracker_endpoint():
  '''
  Get the endpoint for heron tracker
  :return:
  '''
  return options.tracker_url


def create_url(fmt):
  '''
  Given an URL format, substitute with tracker service endpoint
  :param fmt:
  :return:
  '''
  return fmt % get_tracker_endpoint()


@tornado.gen.coroutine
def get_clusters():
  '''
  :return:
  '''
  request_url = create_url(CLUSTER_URL_FMT)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_topologies():
  '''
  Get the list of topologies given a data center from heron tracker
  :return:
  '''
  request_url = create_url(TOPOLOGIES_URL_FMT)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_topologies_states():
  '''
  Get the list of topologies and their states
  :return:
  '''
  request_url = create_url(TOPOLOGIES_STATS_URL_FMT)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


@tornado.gen.coroutine
def _get_topologies(cluster, role=None, env=None):
  endpoint = create_url(TOPOLOGIES_URL_FMT)
  params = dict(cluster=cluster)
  if role is not None:
    params['role'] = role
  if env is not None:
    params['environ'] = env
  request_url = tornado.httputil.url_concat(endpoint, params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
def get_cluster_topologies(cluster):
  '''
  Get the list of topologies given a cluster
  :param cluster:
  :return:
  '''
  return _get_topologies(cluster)


################################################################################
def get_cluster_role_topologies(cluster, role):
  '''
  Get the list of topologies given a cluster submitted by a given role
  :param cluster:
  :param role:
  :return:
  '''
  return _get_topologies(cluster, role=role)


################################################################################
def get_cluster_role_env_topologies(cluster, role, env):
  '''
  Get the list of topologies given a cluster submitted by a given role under a given environment
  :param cluster:
  :param role:
  :param env:
  :return:
  '''
  return _get_topologies(cluster, role=role, env=env)


################################################################################
@tornado.gen.coroutine
def get_execution_state(cluster, environ, topology, role=None):
  '''
  Get the execution state of a topology in a cluster
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(create_url(EXECUTION_STATE_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_logical_plan(cluster, environ, topology, role=None):
  '''
  Get the logical plan state of a topology in a cluster
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(LOGICALPLAN_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_comps(cluster, environ, topology, role=None):
  '''
  Get the list of component names for the topology from Heron Nest
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(LOGICALPLAN_URL_FMT), params)
  lplan = yield fetch_url_as_json(request_url)
  comps = list(lplan['spouts'].keys()) + list(lplan['bolts'].keys())
  raise tornado.gen.Return(comps)

################################################################################
@tornado.gen.coroutine
def get_instances(cluster, environ, topology, role=None):
  '''
  Get the list of instances for the topology from Heron Nest
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(PHYSICALPLAN_URL_FMT), params)
  pplan = yield fetch_url_as_json(request_url)
  instances = list(pplan['instances'].keys())
  raise tornado.gen.Return(instances)


################################################################################
@tornado.gen.coroutine
def get_packing_plan(cluster, environ, topology, role=None):
  '''
  Get the packing plan state of a topology in a cluster from tracker
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(PACKINGPLAN_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_physical_plan(cluster, environ, topology, role=None):
  '''
  Get the physical plan state of a topology in a cluster from tracker
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(PHYSICALPLAN_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_scheduler_location(cluster, environ, topology, role=None):
  '''
  Get the scheduler location of a topology in a cluster from tracker
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(cluster=cluster, environ=environ, topology=topology)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(SCHEDULER_LOCATION_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_component_exceptionsummary(cluster, environ, topology, component, role=None):
  '''
  Get summary of exception for a component
  :param cluster:
  :param environ:
  :param topology:
  :param component:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      component=component)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(EXCEPTION_SUMMARY_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_component_exceptions(cluster, environ, topology, component, role=None):
  '''
  Get exceptions for 'component' for 'topology'
  :param cluster:
  :param environ:
  :param topology:
  :param component:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      component=component)
  if role is not None:
    params['role'] = role
  request_url = tornado.httputil.url_concat(
      create_url(EXCEPTIONS_URL_FMT), params)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_comp_instance_metrics(cluster, environ, topology, component,
                              metrics, instances, time_range, role=None):
  '''
  Get the metrics for some instances of a topology from tracker
  :param cluster:
  :param environ:
  :param topology:
  :param component:
  :param metrics:     dict of display name to cuckoo name
  :param instances:
  :param time_range:  2-tuple consisting of start and end of range
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      component=component)
  if role is not None:
    params['role'] = role

  # form the fetch url
  request_url = tornado.httputil.url_concat(
      create_url(METRICS_URL_FMT), params)

  # convert a single instance to a list, if needed
  all_instances = instances if isinstance(instances, list) else [instances]

  # append each metric to the url
  for _, metric_name in list(metrics.items()):
    request_url = tornado.httputil.url_concat(request_url, dict(metricname=metric_name[0]))

  # append each instance to the url
  for i in all_instances:
    request_url = tornado.httputil.url_concat(request_url, dict(instance=i))

  # append the time interval to the url
  request_url = tornado.httputil.url_concat(request_url, dict(interval=time_range[1]))

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_comp_metrics(cluster, environ, topology, component,
                     instances, metricnames, time_range, role=None):
  '''
  Get the metrics for all the instances of a topology from Heron Nest
  :param cluster:
  :param environ:
  :param topology:
  :param component:
  :param instances:
  :param metricnames:   dict of display name to cuckoo name
  :param time_range:    2-tuple consisting of start and end of range
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      component=component)
  if role is not None:
    params['role'] = role

  # form the url
  request_url = tornado.httputil.url_concat(
      create_url(METRICS_URL_FMT), params)

  # append each metric to the url
  for metric_name in metricnames:
    request_url = tornado.httputil.url_concat(request_url, dict(metricname=metric_name))

  # append each instance to the url
  for instance in instances:
    request_url = tornado.httputil.url_concat(request_url, dict(instance=instance))

  # append the time interval to the url
  request_url = tornado.httputil.url_concat(request_url, dict(interval=time_range[1]))

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_metrics(cluster, environment, topology, timerange, query, role=None):
  '''
  Get the metrics for a topology from tracker
  :param cluster:
  :param environment:
  :param topology:
  :param timerange:
  :param query:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environment,
      topology=topology,
      starttime=timerange[0],
      endtime=timerange[1],
      query=query)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(
      create_url(METRICS_QUERY_URL_FMT), params
  )

  logging.info("get_metrics %s", request_url)
  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


################################################################################
@tornado.gen.coroutine
def get_comp_metrics_timeline(cluster, environ, topology, component,
                              instances, metricnames, time_range, role=None):
  '''
  Get the minute-by-minute metrics for all instances of a topology from tracker
  :param cluster:
  :param environ:
  :param topology:
  :param component:
  :param instances:
  :param metricnames:   dict of display name to cuckoo name
  :param time_range:    2-tuple consisting of start and end of range
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      component=component)

  if role is not None:
    params['role'] = role

  # form the url
  request_url = tornado.httputil.url_concat(create_url(METRICS_TIMELINE_URL_FMT), params)

  if role is not None:
    request_url = tornado.httputil.url_concat(request_url, dict(role=role))

  # append each metric to the url
  for metric_name in metricnames:
    request_url = tornado.httputil.url_concat(request_url, dict(metricname=metric_name))

  # append each instance to the url
  for instance in instances:
    request_url = tornado.httputil.url_concat(request_url, dict(instance=instance))

  # append the time interval to the url
  request_url = tornado.httputil.url_concat(
      request_url, dict(starttime=time_range[0], endtime=time_range[1]))

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


@tornado.gen.coroutine
def get_topology_info(cluster, environ, topology, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(create_url(INFO_URL_FMT), params)

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


# Get pid of the instance
@tornado.gen.coroutine
def get_instance_pid(cluster, environ, topology, instance, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param instance:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      instance=instance)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(create_url(PID_URL_FMT), params)

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


# Get jstack of instance
@tornado.gen.coroutine
def get_instance_jstack(cluster, environ, topology, instance, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param instance:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      instance=instance)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(
      create_url(JSTACK_URL_FMT), params)

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


# Get histogram of active memory objects.
@tornado.gen.coroutine
def get_instance_mem_histogram(cluster, environ, topology, instance, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param instance:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      instance=instance)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(
      create_url(HISTOGRAM_URL_FMT), params)

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


# Call heap dump for an instance and save it at /tmp/heap.bin
@tornado.gen.coroutine
def run_instance_jmap(cluster, environ, topology, instance, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param instance:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      instance=instance)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(
      create_url(JMAP_URL_FMT), params)

  if role is not None:
    request_url = tornado.httputil.url_concat(request_url, dict(role=role))

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))

# Get a url to download a file from the container
def get_container_file_download_url(cluster, environ, topology, container,
                                    path, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param container:
  :param path:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      container=container,
      path=path)
  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(
      create_url(FILE_DOWNLOAD_URL_FMT), params)

  if role is not None:
    request_url = tornado.httputil.url_concat(request_url, dict(role=role))
  return request_url

# Get file data from the container
@tornado.gen.coroutine
def get_container_file_data(cluster, environ, topology, container,
                            path, offset, length, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param container:
  :param path:
  :param offset:
  :param length:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      container=container,
      path=path,
      offset=offset,
      length=length)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(
      create_url(FILE_DATA_URL_FMT), params)

  if role is not None:
    request_url = tornado.httputil.url_concat(request_url, dict(role=role))

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


# Get filestats
@tornado.gen.coroutine
def get_filestats(cluster, environ, topology, container, path, role=None):
  '''
  :param cluster:
  :param environ:
  :param topology:
  :param container:
  :param path:
  :param role:
  :return:
  '''
  params = dict(
      cluster=cluster,
      environ=environ,
      topology=topology,
      container=container,
      path=path)

  if role is not None:
    params['role'] = role

  request_url = tornado.httputil.url_concat(create_url(FILESTATS_URL_FMT), params)

  raise tornado.gen.Return((yield fetch_url_as_json(request_url)))


class HeronQueryHandler(QueryHandler):
  ''' HeronQueryHandler '''

  @tornado.gen.coroutine
  def fetch(self, cluster, metric, topology, component, instance, timerange, environ=None):
    '''
    :param cluster:
    :param metric:
    :param topology:
    :param component:
    :param instance:
    :param timerange:
    :param environ:
    :return:
    '''
    components = [component] if component != "*" else (yield get_comps(cluster, environ, topology))

    futures = []
    for comp in components:
      query = self.get_query(metric, comp, instance)
      future = get_metrics(cluster, environ, topology, timerange, query)
      futures.append(future)

    results = yield futures

    timelines = []
    for result in results:
      timelines.extend(result["timeline"])

    result = self.get_metric_response(timerange, timelines, False)

    raise tornado.gen.Return(result)

  @tornado.gen.coroutine
  def fetch_max(self, cluster, metric, topology, component, instance, timerange, environ=None):
    '''
    :param cluster:
    :param metric:
    :param topology:
    :param component:
    :param instance:
    :param timerange:
    :param environ:
    :return:
    '''
    components = [component] if component != "*" else (yield get_comps(cluster, environ, topology))

    result = {}
    futures = []
    for comp in components:
      query = self.get_query(metric, comp, instance)
      max_query = "MAX(%s)" % query
      future = get_metrics(cluster, environ, topology, timerange, max_query)
      futures.append(future)

    results = yield futures

    data = self.compute_max(results)

    result = self.get_metric_response(timerange, data, True)

    raise tornado.gen.Return(result)

  # pylint: disable=unused-argument
  @tornado.gen.coroutine
  def fetch_backpressure(self, cluster, metric, topology, component, instance, \
    timerange, is_max, environ=None):
    '''
    :param cluster:
    :param metric:
    :param topology:
    :param component:
    :param instance:
    :param timerange:
    :param isMax:
    :param environ:
    :return:
    '''
    instances = yield get_instances(cluster, environ, topology)
    if component != "*":
      filtered_inst = [instance for instance in instances if instance.split("_")[2] == component]
    else:
      filtered_inst = instances

    futures_dict = {}
    for inst in filtered_inst:
      query = queries.get(metric).format(inst)
      futures_dict[inst] = get_metrics(cluster, environ, topology, timerange, query)

    res = yield futures_dict

    if not is_max:
      timelines = []
      for key in res:
        result = res[key]
        # Replacing stream manager instance name with component instance name
        if len(result["timeline"]) > 0:
          result["timeline"][0]["instance"] = key
        timelines.extend(result["timeline"])
      result = self.get_metric_response(timerange, timelines, is_max)
    else:
      data = self.compute_max(list(res.values()))
      result = self.get_metric_response(timerange, data, is_max)

    raise tornado.gen.Return(result)

  # pylint: disable=no-self-use
  def compute_max(self, multi_ts):
    '''
    :param multi_ts:
    :return:
    '''
    # Some components don't have specific metrics such as capacity hence the
    # key set is empty. These components are filtered out first.
    filtered_ts = [ts for ts in multi_ts if len(ts["timeline"][0]["data"]) > 0]
    if len(filtered_ts) > 0 and len(filtered_ts[0]["timeline"]) > 0:
      keys = list(filtered_ts[0]["timeline"][0]["data"].keys())
      timelines = ([res["timeline"][0]["data"][key] for key in keys] for res in filtered_ts)
      values = (max(v) for v in zip(*timelines))
      return dict(list(zip(keys, values)))
    return {}

  # pylint: disable=no-self-use
  def get_metric_response(self, timerange, data, isMax):
    '''
    :param timerange:
    :param data:
    :param isMax:
    :return:
    '''
    if isMax:
      return dict(
          status="success",
          starttime=timerange[0],
          endtime=timerange[1],
          result=dict(timeline=[dict(data=data)])
      )

    return dict(
        status="success",
        starttime=timerange[0],
        endtime=timerange[1],
        result=dict(timeline=data)
    )

  # pylint: disable=no-self-use
  def get_query(self, metric, component, instance):
    '''
    :param metric:
    :param component:
    :param instance:
    :return:
    '''
    q = queries.get(metric)
    return q.format(component, instance)
