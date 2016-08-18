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
''' fetch.py '''
import json
import time

import tornado.httpclient
import tornado.gen

from heron.common.src.python.utils import log

Log = log.Log

################################################################################

################################################################################
@tornado.gen.coroutine
def fetch_url_as_json(fetch_url, default_value=None):
  '''
  Fetch the given url and convert the response to json.
  :param fetch_url: URL to fetch
  :param default_value: value to return in case of failure
  :return:
  '''
  # assign empty dict for optional param
  if default_value is None:
    default_value = dict()

  Log.debug("fetching url %s", fetch_url)
  ret = default_value

  # time the duration of the fetch
  start = time.time()

  # fetch the URL asynchronously
  http_response = yield tornado.httpclient.AsyncHTTPClient().fetch(fetch_url)

  # handle http errors, and return if any
  if http_response.error:
    Log.error("Unable to get response from %s. Error %s", fetch_url, http_response.error)
    raise tornado.gen.Return(ret)

  # load response and handle return errors, if any
  response = json.loads(http_response.body)
  if not 'result' in response:
    Log.error("Empty response from %s", fetch_url)
    raise tornado.gen.Return(ret)

  # get the response and execution time on server side
  ret = response['result']
  execution = 1000 * response['executiontime']

  # calculate the time
  end = time.time()
  duration = 1000 * (end - start)

  Log.debug("TIME: url fetch took %.2f ms server time %s", execution, fetch_url)
  Log.debug("TIME: url fetch took %.2f ms round trip  %s", duration, fetch_url)

  # convert future to value
  raise tornado.gen.Return(ret)
