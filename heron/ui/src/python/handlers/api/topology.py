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

import json
import os
import random
import signal
import sys
import time
import tornado.escape
import tornado.web
import tornado.gen
import urllib2
import datetime

import metrics
from heron.ui.src.python.handlers import base
from heron.ui.src.python.handlers import access
from heron.ui.src.python.handlers import common

class TopologyExceptionSummaryHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology, comp_name):
        start_time = time.time()
        comp_names = []
        if comp_name == "All":
            lplan = yield access.get_logical_plan(cluster, environ, topology)
            if not lplan:
                self.write(dict())
                return

            if not lplan.has_key('spouts') or not lplan.has_key('bolts'):
                self.write(dict())
                return
            comp_names = lplan['spouts'].keys()
            comp_names.extend(lplan['bolts'].keys())
        else:
            comp_names = [comp_name]
        exception_infos = dict()
        for comp_name in comp_names:
            exception_infos[comp_name] = yield access.get_component_exceptionsummary(
                cluster, environ, topology, comp_name)

        # Combine exceptions from multiple component
        aggregate_exceptions = dict()
        for comp_name, exception_logs in exception_infos.items():
            for exception_log in exception_logs:
                class_name = exception_log['class_name'];
                if class_name != '':
                    if not class_name in aggregate_exceptions:
                        aggregate_exceptions[class_name] = 0;
                    aggregate_exceptions[class_name] += int(exception_log['count'])
        # Put the exception value in a table
        aggregate_exceptions_table = []
        for key in aggregate_exceptions.keys():
            aggregate_exceptions_table.append([key, str(aggregate_exceptions[key])])
        result = dict(
            status = "success",
            executiontime = time.time() - start_time,
            result = aggregate_exceptions_table)
        self.write(result)

class ListTopologiesJsonHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self):

        # get all the topologies from heron nest
        topologies = yield access.get_topologies_states()

        result = dict()

        # now convert some of the fields to be displayable
        for cluster, cluster_value in topologies.iteritems():
            result[cluster] = dict()
            for environ, environ_value in cluster_value.iteritems():
                result[cluster][environ] = dict()
                for topology, topology_value in environ_value.iteritems():
                    if "jobname" not in topology_value or topology_value["jobname"] is None: continue

                    if topology_value.has_key("submission_time"):
                        topology_value["submission_time"] = topology_value["submission_time"]
                    else:
                        topology_value["submission_time"] = '-'

                    result[cluster][environ][topology] = topology_value

        self.write(result)

class TopologyLogicalPlanJsonHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology):

        start_time = time.time()
        lplan = yield access.get_logical_plan(cluster, environ, topology)

        if not lplan:
            self.write(dict())
            return

        if not lplan.has_key('spouts') or  not lplan.has_key('bolts'):
            self.write(dict())
            return

        # format the logical plan as required by the web (because of Ambrose)
        # first, spouts followed by bolts
        spouts_map = dict()
        for sn, sv in lplan['spouts'].items():
            spouts_map[sn] = dict(
                outputs = sv["outputs"],
                spout_type = sv["type"],
                spout_source = sv["source"],
            )

        bolts_map = dict()
        for bn, bv in lplan['bolts'].items():
            bolts_map[bn] = dict(
                inputComponents = [i['component_name'] for i in bv['inputs']],
                inputs = bv["inputs"],
                outputs = bv["outputs"]
            )

        diameter = common.graph.TopologyDAG(lplan).diameter()
        # construct the result
        result = dict(
            status = "success",
            message = "",
            version = common.VERSION,
            executiontime = time.time() - start_time,
            result = dict(
                stages = diameter,
                spouts = spouts_map,
                bolts = bolts_map
            )
        )

        self.write(result)

class TopologyPhysicalPlanJsonHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology):

        start_time = time.time()
        pplan = yield access.get_physical_plan(cluster, environ, topology)

        result_map = dict(
            status = "success",
            message = "",
            version = common.VERSION,
            executiontime = time.time() - start_time,
            result = pplan
        )

        self.write(result_map)

class TopologyExecutionStateJsonHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology):

        start_time = time.time()
        estate = yield access.get_execution_state(cluster, environ, topology)

        result_map = dict(
            status = "success",
            message = "",
            version = common.VERSION,
            executiontime = time.time() - start_time,
            result = estate
        )

        self.write(result_map)

class TopologySchedulerLocationJsonHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology):

        start_time = time.time()
        scheduler_location = yield access.get_scheduler_location(cluster, environ, topology)

        result_map = dict(
            status = "success",
            message = "",
            version = common.VERSION,
            executiontime = time.time() - start_time,
            result = estate
        )

        self.write(result_map)


# Handler for getting exceptions
class TopologyExceptionsJsonHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology, component):
      start_time = time.time()
      futures = yield access.get_component_exceptions(cluster, environ, topology, component)
      result_map = dict(
        status = 'success',
        executiontime = time.time() - start_time,
        result = futures)
      self.write(json.dumps(result_map))

class PidHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology, instance):
        pplan = yield access.get_physical_plan(cluster, environ, topology)
        host = pplan['stmgrs'][pplan['instances'][instance]['stmgrId']]['host']
        result = json.loads((yield access.get_instance_pid(
            cluster, environ, topology, instance)))
        self.write('<pre><br/>$%s>: %s<br/><br/>%s</pre>' % (
            host,
            tornado.escape.xhtml_escape(result['command']),
            tornado.escape.xhtml_escape(result['stdout'])))

class JstackHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology, instance):
        pplan = yield access.get_physical_plan(cluster, environ, topology)
        host = pplan['stmgrs'][pplan['instances'][instance]['stmgrId']]['host']
        result = json.loads((yield access.get_instance_jstack(
            cluster, environ, topology, instance)))
        self.write('<pre><br/>$%s>: %s<br/><br/>%s</pre>' % (
            host,
            tornado.escape.xhtml_escape(result['command']),
            tornado.escape.xhtml_escape(result['stdout'])))

class MemoryHistogramHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology, instance):
        pplan = yield access.get_physical_plan(cluster, environ, topology)
        host = pplan['stmgrs'][pplan['instances'][instance]['stmgrId']]['host']
        result = json.loads((yield access.get_instance_mem_histogram(
            cluster, environ, topology, instance)))
        self.write('<pre><br/>$%s>: %s<br/><br/>%s</pre>' % (
            host,
            tornado.escape.xhtml_escape(result['command']),
            tornado.escape.xhtml_escape(result['stdout'])))

class JmapHandler(base.BaseHandler):
    @tornado.gen.coroutine
    def get(self, cluster, environ, topology, instance):
        pplan = yield access.get_physical_plan(cluster, environ, topology)
        host = pplan['stmgrs'][pplan['instances'][instance]['stmgrId']]['host']
        result = json.loads((yield access.run_instance_jmap(
            cluster, environ, topology, instance)))
        notes = "<br/>\n".join([
            "* May Take longer than usual (1-2 min) please be patient."
            "* Use scp to copy heap dump files from host. (scp %s:/tmp/heap.bin /tmp/)" % host])
        self.write('<pre>%s<br/>$%s>: %s<br/><br/>%s</pre>' % (
            notes,
            host,
            tornado.escape.xhtml_escape(result['command']),
            tornado.escape.xhtml_escape(result['stdout'])))
