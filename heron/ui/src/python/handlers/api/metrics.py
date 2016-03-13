import os, signal, sys
import time, random
import logging
import json

from heron.ui.src.python.handlers import base
from heron.ui.src.python.handlers import access

import tornado.gen

ALL_INSTANCES = '*'

query_handler = access.HeronQueryHandler()

class MetricsHandler(base.BaseHandler):

    @tornado.gen.coroutine
    def get(self):
        cluster = self.get_argument("cluster")
        environ = self.get_argument("environ")
        topology = self.get_argument("topology")
        component = self.get_argument("component", default=None)
        metricnames = self.get_arguments("metricname")
        instances = self.get_arguments("instance")
        interval = self.get_argument("interval", default=-1)
        time_range = (0, interval)
        compnames = [component] if component else (yield access.get_comps(cluster, environ, topology))

        # fetch the metrics
        futures = {}
        for comp in compnames:
            future = access.get_comp_metrics(
                cluster, environ, topology, comp, instances,
                metricnames, time_range)
            futures[comp] = future

        results = yield futures

        self.write(results[component] if component else results)


class MetricsTimelineHandler(base.BaseHandler):

    @tornado.gen.coroutine
    def get(self):
        cluster = self.get_argument("cluster")
        environ = self.get_argument("environ")
        topology = self.get_argument("topology")
        component = self.get_argument("component", default=None)
        metric = self.get_argument("metric")
        instances = self.get_argument("instance")
        start = self.get_argument("starttime")
        end = self.get_argument("endtime")
        maxquery = self.get_argument("max", default=False)
        timerange = (start, end)
        compnames = [component]

        # fetch the metrics
        futures = {}
        fetch = query_handler.fetch_max if maxquery else query_handler.fetch
        for comp in compnames:
            future = fetch(cluster, metric, topology, component,
                        instances, timerange, environ)
            futures[comp] = future

        results = yield futures
        self.write(results[component] if component else results)
