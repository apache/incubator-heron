''' main.py '''
import logging
import os
import sys
import tornado.ioloop
import tornado.escape
import tornado.web

RESULTS_DIRECTORY = "results"

class MainHandler(tornado.web.RequestHandler):
  '''main handler'''
  def get(self):
    ''' get method'''
    self.write("Heron integration-test helper")

class FileHandler(tornado.web.RequestHandler):
  ''' file handler '''
  def get(self, fileName):
    ''' get method'''
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
    ''' post '''
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

def main():
  '''
  Runs a tornado http server that listens for any
  integration test josn result get/post requests
  '''

  root = logging.getLogger()
  root.setLevel(logging.DEBUG)

  if not os.path.exists(RESULTS_DIRECTORY):
    os.makedirs(RESULTS_DIRECTORY)

  application = tornado.web.Application([
      (r"/", MainHandler),
      (r"^/results/([a-zA-Z0-9_-]+$)", FileHandler)
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
