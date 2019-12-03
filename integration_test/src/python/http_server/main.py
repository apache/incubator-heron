# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
''' main.py '''
import logging
import os
import sys
import tornado.ioloop
import tornado.escape
import tornado.web

from heron.common.src.python.utils import log

RESULTS_DIRECTORY = "results"

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    self.write("Heron integration-test helper")

class FileHandler(tornado.web.RequestHandler):
  def get(self, fileName):
    jsonFilePath = RESULTS_DIRECTORY + "/" + fileName + ".json"

    if not os.path.exists(jsonFilePath):
      self.clear()
      self.set_status(404)
      self.finish("%s does not exist" % (fileName + ".json"))
    else:
      with open(jsonFilePath, "r") as jsonFile:
        data = jsonFile.read()

      self.set_header("Content-Type", 'application/json; charset="utf-8"')
      self.write(data)

  def post(self, fileName):
    jsonFilePath = RESULTS_DIRECTORY + "/" + fileName + ".json"

    #Overwrites the existing file
    with open(jsonFilePath, "w") as jsonFile:
      try:
        data = tornado.escape.json_decode(self.request.body)
        jsonFile.write(tornado.escape.json_encode(data))
        self.write("Results written to " + jsonFilePath + " successfully")
      except ValueError as e:
        logging.error("ValueError: " + str(e))
        self.clear()
        self.set_status(400)
        self.finish("Invalid Json")

class MemoryMapGetAllHandler(tornado.web.RequestHandler):
  def initialize(self, state_map):
    self.state_map = state_map

  def get(self):
    self.set_header("Content-Type", 'application/json; charset="utf-8"')
    self.write(tornado.escape.json_encode(self.state_map))

class MemoryMapHandler(tornado.web.RequestHandler):
  def initialize(self, state_map):
    self.state_map = state_map

  def get(self, key):
    if key:
      self.set_header("Content-Type", 'application/json; charset="utf-8"')
      if key in self.state_map:
        self.write(self.state_map[key])
      else:
        raise tornado.web.HTTPError(status_code=404, log_message="Key %s not found" % key)
    else:
      self.write(str(self.state_map))

  def post(self, key):
    data = tornado.escape.json_decode(self.request.body)
    self.state_map[key] = tornado.escape.json_encode(data)
    self.write("Results written to " + tornado.escape.json_encode(self.state_map) + " successfully")

# for instance states in stateful processing
class StateResultHandler(tornado.web.RequestHandler):
  def initialize(self, result_map):
    self.result_map = result_map

  def get(self, key):
    if key:
      self.set_header("Content-Type", 'application/json; charset="utf-8"')
      if key in self.result_map:
        self.write(tornado.escape.json_encode(self.result_map[key]))
      else:
        raise tornado.web.HTTPError(status_code=404, log_message="Key %s not found" % key)
    else:
      self.write(tornado.escape.json_encode(self.result_map))

  def post(self, key):
    data = tornado.escape.json_decode(self.request.body)
    if key:
      if key in self.result_map:
        # fix the duplicate record issue
        for comp in self.result_map[key]:
          if list(comp.keys())[0] == list(data.keys())[0]:
            break
        else:
          self.result_map[key].append(data)
      else:
        self.result_map[key] = [data]
      self.write("Results written successfully: topology " + key + ' instance ' + list(data.keys())[0])
    else:
      raise tornado.web.HTTPError(status_code=404, log_message="Invalid key %s" % key)

def main():
  '''
  Runs a tornado http server that listens for any
  integration test json result get/post requests
  '''

  log.configure(level=logging.DEBUG)

  if not os.path.exists(RESULTS_DIRECTORY):
    os.makedirs(RESULTS_DIRECTORY)

  state_map = {}
  # for instance states in stateful processing
  state_result_map = {}
  application = tornado.web.Application([
    (r"/", MainHandler),
    (r"^/results/([a-zA-Z0-9_-]+$)", FileHandler),
    (r"^/state", MemoryMapGetAllHandler, dict(state_map=state_map)),
    (r"^/state/([a-zA-Z0-9_-]+$)", MemoryMapHandler, dict(state_map=state_map)),
    (r"^/stateResults/([a-zA-Z0-9_-]+$)", StateResultHandler, dict(result_map=state_result_map)),
  ])

  if len(sys.argv) == 1:
    logging.error("Missing argument: port addresss")
    sys.exit(1)

  try:
    port = int(sys.argv[1])
  except Exception as e:
    logging.error("Exception: %s \nprovide a valid port address", e)
    sys.exit(1)

  logging.info("Starting server at port " + str(port))
  application.listen(port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
  main()
