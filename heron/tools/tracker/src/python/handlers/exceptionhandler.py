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
""" exceptionhandler.py """
import traceback
import tornado.gen
import tornado.web

from heron.common.src.python.utils.log import Log
from heron.proto import common_pb2
from heron.proto import tmaster_pb2
from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python.handlers import BaseHandler


# pylint: disable=attribute-defined-outside-init
class ExceptionHandler(BaseHandler):
  """
  URL - /topologies/exceptions?cluster=<cluster>&topology=<topology> \
        &environ=<environment>&component=<component>
  Parameters:
   - cluster - Name of cluster.
   - environ - Running environment.
   - role - (optional) Role used to submit the topology.
   - topology - Name of topology (Note: Case sensitive. Can only
                include [a-zA-Z0-9-_]+)
   - component - Component name
   - instance - (optional, repeated)

  Returns all exceptions for the component of the topology.
  """
  def initialize(self, tracker):
    """ initialize """
    self.tracker = tracker

  @tornado.gen.coroutine
  def get(self):
    """ get method """
    try:
      cluster = self.get_argument_cluster()
      environ = self.get_argument_environ()
      role = self.get_argument_role()
      topName = self.get_argument_topology()
      component = self.get_argument_component()
      topology = self.tracker.getTopologyByClusterRoleEnvironAndName(
          cluster, role, environ, topName)
      instances = self.get_arguments(constants.PARAM_INSTANCE)
      exceptions_logs = yield tornado.gen.Task(self.getComponentException,
                                               topology.tmaster, component, instances)
      self.write_success_response(exceptions_logs)
    except Exception as e:
      Log.debug(traceback.format_exc())
      self.write_error_response(e)

  # pylint: disable=bad-option-value, dangerous-default-value, no-self-use,
  # pylint: disable=unused-argument
  @tornado.gen.coroutine
  def getComponentException(self, tmaster, component_name, instances=[], callback=None):
    """
    Get all (last 1000) exceptions for 'component_name' of the topology.
    Returns an Array of exception logs on success.
    Returns json with message on failure.
    """
    if not tmaster or not tmaster.host or not tmaster.stats_port:
      return

    exception_request = tmaster_pb2.ExceptionLogRequest()
    exception_request.component_name = component_name
    if len(instances) > 0:
      exception_request.instances.extend(instances)
    request_str = exception_request.SerializeToString()
    port = str(tmaster.stats_port)
    host = tmaster.host
    url = "http://{0}:{1}/exceptions".format(host, port)
    request = tornado.httpclient.HTTPRequest(url,
                                             body=request_str,
                                             method='POST',
                                             request_timeout=5)
    Log.debug('Making HTTP call to fetch exceptions url: %s', url)
    try:
      client = tornado.httpclient.AsyncHTTPClient()
      result = yield client.fetch(request)
      Log.debug("HTTP call complete.")
    except tornado.httpclient.HTTPError as e:
      raise Exception(str(e))

    # Check the response code - error if it is in 400s or 500s
    responseCode = result.code
    if responseCode >= 400:
      message = "Error in getting exceptions from Tmaster, code: " + responseCode
      Log.error(message)
      raise tornado.gen.Return({
          "message": message
      })

    # Parse the response from tmaster.
    exception_response = tmaster_pb2.ExceptionLogResponse()
    exception_response.ParseFromString(result.body)

    if exception_response.status.status == common_pb2.NOTOK:
      if exception_response.status.HasField("message"):
        raise tornado.gen.Return({
            "message": exception_response.status.message
        })

    # Send response
    ret = []
    for exception_log in exception_response.exceptions:
      ret.append({'hostname': exception_log.hostname,
                  'instance_id': exception_log.instance_id,
                  'stack_trace': exception_log.stacktrace,
                  'lasttime': exception_log.lasttime,
                  'firsttime': exception_log.firsttime,
                  'count': str(exception_log.count),
                  'logging': exception_log.logging})
    raise tornado.gen.Return(ret)
