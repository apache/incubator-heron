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

import tornado.gen

from heron.tracker.src.python import constants
from heron.tracker.src.python.handlers import BaseHandler

class ClustersHandler(BaseHandler):
  """
  URL - /clusters

  The response JSON is a dict with following format:
  {
    "clusters": [
      {
        "name": "cluster1",
        "tags": ["tag1", "tag2"]
      },
      {
        "name": "cluster2",
        "tags": ["tag1", "tag2"]
      }
    ]
  }
  """

  def initialize(self, tracker):
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    clusters = dict()
    for tp in self.tracker.topologies:
      name = tp.cluster
      if name:
        cluster = clusters.get(name, dict(name="", tags=set()))
        cluster["name"] = name
        if tp.environ:
          cluster["tags"].add(tp.environ)

        clusters[name] = cluster


    clusters = list(dict(name=v["name"], tags=list(v["tags"])) for k, v in clusters.iteritems())
    self.write_success_response(dict(clusters=clusters))
