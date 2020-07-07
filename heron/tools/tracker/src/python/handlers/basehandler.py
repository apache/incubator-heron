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

''' basehandlers.py '''
import time
import tornado.escape
import tornado.web

from heron.tools.tracker.src.python import constants

# pylint: disable=too-many-public-methods
# pylint: disable=abstract-method
class BaseHandler(tornado.web.RequestHandler):
  """
  Base Handler. All the other handlers derive from
  this class. This exposes some base methods,
  like making response for success and failure cases,
  and measuring times.
  """

  def set_default_headers(self):
    ''' Allow any domain to make queries to tracker. '''
    self.set_header("Access-Control-Allow-Origin", "*")

  # pylint: disable=attribute-defined-outside-init
  def prepare(self):
    """
    Used for timing. Sets the basehandler_starttime to current time, and
    is used when writing the response back.
    Subclasses of BaseHandler must never use self.write, but instead use
    self.write_error or self.write_result methods to correctly include
    the timing.
    """
    self.basehandler_starttime = time.time()

  def write_success_response(self, result):
    """
    Result may be a python dictionary, array or a primitive type
    that can be converted to JSON for writing back the result.
    """
    response = self.make_success_response(result)
    now = time.time()
    spent = now - self.basehandler_starttime
    response[constants.RESPONSE_KEY_EXECUTION_TIME] = spent
    self.write_json_response(response)

  def write_error_response(self, message):
    """
    Writes the message as part of the response and sets 404 status.
    """
    self.set_status(404)
    response = self.make_error_response(str(message))
    now = time.time()
    spent = now - self.basehandler_starttime
    response[constants.RESPONSE_KEY_EXECUTION_TIME] = spent
    self.write_json_response(response)

  def write_json_response(self, response):
    """ write back json response """
    self.write(tornado.escape.json_encode(response))
    self.set_header("Content-Type", "application/json")

  # pylint: disable=no-self-use
  def make_response(self, status):
    """
    Makes the base dict for the response.
    The status is the string value for
    the key "status" of the response. This
    should be "success" or "failure".
    """
    response = {
        constants.RESPONSE_KEY_STATUS: status,
        constants.RESPONSE_KEY_VERSION: constants.API_VERSION,
        constants.RESPONSE_KEY_EXECUTION_TIME: 0,
        constants.RESPONSE_KEY_MESSAGE: "",
    }
    return response

  def make_success_response(self, result):
    """
    Makes the python dict corresponding to the
    JSON that needs to be sent for a successful
    response. Result is the actual payload
    that gets sent.
    """
    response = self.make_response(constants.RESPONSE_STATUS_SUCCESS)
    response[constants.RESPONSE_KEY_RESULT] = result
    return response

  def make_error_response(self, message):
    """
    Makes the python dict corresponding to the
    JSON that needs to be sent for a failed
    response. Message is the message that is
    sent as the reason for failure.
    """
    response = self.make_response(constants.RESPONSE_STATUS_FAILURE)
    response[constants.RESPONSE_KEY_MESSAGE] = message
    return response

  def get_argument_cluster(self):
    """
    Helper function to get request argument.
    Raises exception if argument is missing.
    Returns the cluster argument.
    """
    try:
      return self.get_argument(constants.PARAM_CLUSTER)
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_role(self):
    """
    Helper function to get request argument.
    Raises exception if argument is missing.
    Returns the role argument.
    """
    try:
      return self.get_argument(constants.PARAM_ROLE, default=None)
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)


  def get_argument_environ(self):
    """
    Helper function to get request argument.
    Raises exception if argument is missing.
    Returns the environ argument.
    """
    try:
      return self.get_argument(constants.PARAM_ENVIRON)
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_topology(self):
    """
    Helper function to get topology argument.
    Raises exception if argument is missing.
    Returns the topology argument.
    """
    try:
      topology = self.get_argument(constants.PARAM_TOPOLOGY)
      return topology
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_component(self):
    """
    Helper function to get component argument.
    Raises exception if argument is missing.
    Returns the component argument.
    """
    try:
      component = self.get_argument(constants.PARAM_COMPONENT)
      return component
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_instance(self):
    """
    Helper function to get instance argument.
    Raises exception if argument is missing.
    Returns the instance argument.
    """
    try:
      instance = self.get_argument(constants.PARAM_INSTANCE)
      return instance
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_starttime(self):
    """
    Helper function to get starttime argument.
    Raises exception if argument is missing.
    Returns the starttime argument.
    """
    try:
      starttime = self.get_argument(constants.PARAM_STARTTIME)
      return starttime
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_endtime(self):
    """
    Helper function to get endtime argument.
    Raises exception if argument is missing.
    Returns the endtime argument.
    """
    try:
      endtime = self.get_argument(constants.PARAM_ENDTIME)
      return endtime
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_query(self):
    """
    Helper function to get query argument.
    Raises exception if argument is missing.
    Returns the query argument.
    """
    try:
      query = self.get_argument(constants.PARAM_QUERY)
      return query
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_offset(self):
    """
    Helper function to get offset argument.
    Raises exception if argument is missing.
    Returns the offset argument.
    """
    try:
      offset = self.get_argument(constants.PARAM_OFFSET)
      return offset
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_argument_length(self):
    """
    Helper function to get length argument.
    Raises exception if argument is missing.
    Returns the length argument.
    """
    try:
      length = self.get_argument(constants.PARAM_LENGTH)
      return length
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def get_required_arguments_metricnames(self):
    """
    Helper function to get metricname arguments.
    Notice that it is get_argument"s" variation, which means that this can be repeated.
    Raises exception if argument is missing.
    Returns a list of metricname arguments
    """
    try:
      metricnames = self.get_arguments(constants.PARAM_METRICNAME)
      if not metricnames:
        raise tornado.web.MissingArgumentError(constants.PARAM_METRICNAME)
      return metricnames
    except tornado.web.MissingArgumentError as e:
      raise Exception(e.log_message)

  def validateInterval(self, startTime, endTime):
    """
    Helper function to validate interval.
    An interval is valid if starttime and endtime are integrals,
    and starttime is less than the endtime.
    Raises exception if interval is not valid.
    """
    start = int(startTime)
    end = int(endTime)
    if start > end:
      raise Exception("starttime is greater than endtime.")
