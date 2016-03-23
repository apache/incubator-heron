import json
import time
import logging

import tornado.httpclient
import tornado.gen

################################################################################
# Fetch the given url and convert the response to json.
# fetch_url     - URL to fetch
# default_value - value to return in case of failure
################################################################################
@tornado.gen.coroutine
def fetch_url_as_json(fetch_url, default_value=dict()):
  logging.info("fetching url %s", fetch_url)
  ret = default_value

  # time the duration of the fetch
  start = time.time()

  # fetch the URL asynchronously
  http_response = yield tornado.httpclient.AsyncHTTPClient().fetch(fetch_url)

  # handle http errors, and return if any
  if http_response.error:
    logging.error(
        "Unable to get response from %s. Error %s", fetch_url, http_response.error)
    raise tornado.gen.Return(ret)

  # load response and handle return errors, if any
  response = json.loads(http_response.body)
  if not 'result' in response:
    logging.error("Empty response from %s", fetch_url)
    raise tornado.gen.Return(ret)

  # get the response and execution time on server side
  ret = response['result']
  execution = 1000 * response['executiontime']

  # calculate the time
  end = time.time()
  duration = 1000 * (end - start)

  logging.info("TIME: url fetch took %.2f ms server time %s" % (execution, fetch_url))
  logging.info("TIME: url fetch took %.2f ms round trip  %s" % (duration, fetch_url))

  # convert future to value
  raise tornado.gen.Return(ret)
