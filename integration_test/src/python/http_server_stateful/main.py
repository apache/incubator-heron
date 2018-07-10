''' main.py '''
import logging
import os
import sys
import tornado.ioloop
import tornado.escape
import tornado.web

from heron.common.src.python.utils import log

RESULTS_DIRECTORY = "topo"

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    self.write("Heron stateful-integration-topology-test helper")

class TopoHandler(tornado.web.RequestHandler):
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

class ResultHandler(tornado.web.RequestHandler):
  def initialize(self, result_map):
    self.result_map = result_map

  def get(self, key):
    if key:
      topoJsonFilePath = RESULTS_DIRECTORY + "/" + key + ".json"
      with open(topoJsonFilePath, "r") as jsonFile:
        topoData = jsonFile.read()
      topoData = tornado.escape.json_decode(topoData)
      if topoData['Done']:
        # finishing collect all the instance state, return results.
        self.set_header("Content-Type", 'application/json; charset="utf-8"')
        if key in self.result_map:
          self.write(tornado.escape.json_encode(self.result_map[key]))
        else:
          raise tornado.web.HTTPError(status_code=404, log_message="Key %s not found" % key)
      else:
        self.clear()
        self.set_status(404)
        self.finish("topology %s instance state collection unfinished" % (key))
    else:
      self.write(tornado.escape.json_encode(self.result_map))

  def post(self, key):
    data = tornado.escape.json_decode(self.request.body)
    instance_id = data.keys()[0]

    topoJsonFilePath = RESULTS_DIRECTORY + "/" + key + ".json"
    with open(topoJsonFilePath, "r") as jsonFile:
      topoData = jsonFile.read()
    topoData = tornado.escape.json_decode(topoData)

    if topoData['Done']:
      self.write("Result of topology " + str(key) + " already exist")
      return
    if instance_id in topoData:
      topoData[instance_id] -= 1
      Done = True
      for instance in topoData:
        if topoData[instance] != 0:
          Done = False
      if Done:
        topoData['Done'] = True
      with open(topoJsonFilePath, "w") as jsonFile:
        jsonFile.write(tornado.escape.json_encode(topoData))

      if key in self.result_map:
        self.result_map[key].append(data)
      else:
        self.result_map[key] = [data]
      self.write("Results written successfully: topology " + key + ' instance ' + data.keys()[0])
    else:
      raise tornado.web.HTTPError(status_code=404, log_message="Instance %s not found" % instance_id)


def main():
  '''
  Runs a tornado http server that listens for
  integration test of instance state in stateful
  processing json result get/post requests
  '''

  log.configure(level=logging.DEBUG)

  if not os.path.exists(RESULTS_DIRECTORY):
    os.makedirs(RESULTS_DIRECTORY)

  result_map = {}
  application = tornado.web.Application([
    (r"/", MainHandler),
    (r"^/results/([a-zA-Z0-9_-]+$)", ResultHandler, dict(result_map=result_map)),
    (r"^/topo/([a-zA-Z0-9_-]+$)", TopoHandler),
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
