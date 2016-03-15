import json
import logging
import os
import pkgutil
import stat
import subprocess
import tornado.ioloop
import tornado.web

from tornado.options import define, options, parse_command_line
from tornado.template import Template

from heron.shell.src.python import utils

def stream_to_string(stream):
  """
  Converts stream to string. Blocks until end of stream
  """
  str_builder = ''
  while True:
    line = stream.readline()
    if not line:
      break
    str_builder += line
  return str_builder

def pipe(in_stream, to_cmd):
  """
  Pipes in_stream from output of previous pipe into to_cmd.
  Returns stdout stream of to_cmd
  """
  p = subprocess.Popen(to_cmd,
                       stdout=subprocess.PIPE,
                       stdin=in_stream)
  return p.stdout

def str_cmd(cmd):
  """
  Runs the command and returns its stdout and stderr.
  """
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  (stdout, stderr) = (stream_to_string(p.stdout), stream_to_string(p.stderr))
  return {'command': ' '.join(cmd), 'stderr': stderr, 'stdout': stdout}

def chain(cmd_list):
  """
  Feed output of one command to the next and return final output
  Returns string output of chained application of commands.
  """
  return {
    'command' : ' | '.join(map(lambda x: ' '.join(x) , cmd_list)),
    'stdout' : stream_to_string(reduce(pipe, [None] + cmd_list))
  }

def get_container_id(instance_id):
  return instance_id.split('_')[1]  # Format: container_<index>_component_name_<index>

def get_asset(asset_name):
  return pkgutil.get_data("heron.shell", os.path.join("assets", asset_name))


class PidHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the process ID for an instance.
  """
  @tornado.web.asynchronous
  def get(self, instance_id):
    self.content_type = 'application/json'
    self.write(json.dumps(chain([
      ['ps', 'auxwwww'],
      ['grep', instance_id],
      ['grep', 'java'],
      ['awk', '{print $2}']])).strip())
    self.finish()

class MemoryHistogramHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the memory histogram of a jvm process given its pid.
  """
  @tornado.web.asynchronous
  def get(self, pid):
    body = str_cmd(['jmap', '-histo', pid])
    self.content_type = 'application/json'
    self.write(json.dumps(body))
    self.finish()

class JmapHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the jmap for a jvm process given its pid.
  """
  @tornado.web.asynchronous
  def get(self, pid):
    str_cmd(['rm', '-rf', '/tmp/heap.bin'])
    body = str_cmd(['jmap', '-dump:format=b,file=/tmp/heap.bin', pid])
    str_cmd(['chmod', '+r', '/tmp/heap.bin'])
    self.content_type = 'application/json'
    self.write(json.dumps(body))
    self.finish()

class JstackHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the jstack for a jvm process given its pid.
  """
  @tornado.web.asynchronous
  def get(self, pid):
    body = str_cmd(['jstack', pid])
    self.content_type = 'application/json'
    self.write(json.dumps(body))
    self.finish()

class BrowseHandler(tornado.web.RequestHandler):
  """
  Responsible for browsing directories.
  """
  @tornado.web.asynchronous
  def get(self, path):
    if not path:
      path = "."
    if path.startswith("/"):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return
    t = Template(get_asset("browse.html"))
    args = dict(
      path = path,
      listing = utils.get_listing(path),
      format_prefix = utils.format_prefix,
      stat = stat,
      get_stat = utils.get_stat,
      os = os,
      css = get_asset("bootstrap.css")
    )
    self.write(t.generate(**args))
    self.finish()

class FileHandler(tornado.web.RequestHandler):
  """
  Responsible for creating the web page for files. The html
  will in turn call the /filedata/ endpoint to get the file data.
  """
  @tornado.web.asynchronous
  def get(self, path):
    t = Template(get_asset("file.html"))
    if path is None:
      self.set_status(404)
      self.write("No such file")
      self.finish()
      return
    if path.startswith("/"):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return
    args = dict(
      filename = path,
      jquery = get_asset("jquery.js"),
      pailer = get_asset("jquery.pailer.js"),
      css = get_asset("bootstrap.css"),
    )
    self.write(t.generate(**args))
    self.finish()

class FileDataHandler(tornado.web.RequestHandler):
  """
  Responsible for reading and returning the file data given the offset
  and length of file to be read.
  """
  @tornado.web.asynchronous
  def get(self, path):
    if path is None:
      return {}
    if path.startswith("/"):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return
    offset = self.get_argument("offset", default=-1)
    length = self.get_argument("length", default=-1)
    if not os.path.isfile(path):
      return {}
    data = utils.read_chunk(path, offset, length)
    self.write(json.dumps(data))
    self.finish()

class DownloadHandler(tornado.web.StaticFileHandler):
  """
  Responsible for downloading the files.
  """
  def set_headers(self):
    self.set_header("Content-Disposition", "attachment")

app = tornado.web.Application([
  (r"^/jmap/([0-9]+$)", JmapHandler),
  (r"^/histo/([0-9]+$)", MemoryHistogramHandler),
  (r"^/jstack/([0-9]+$)", JstackHandler),
  (r"^/pid/(.*)", PidHandler),
  (r"^/browse/(.*)", BrowseHandler),
  (r"^/file/(.*)", FileHandler),
  (r"^/filedata/(.*)", FileDataHandler),
  (r"^/download/(.*)", DownloadHandler, {"path":"."}),
])


if __name__ == '__main__':
  define("port", default=9999, help="Runs on the given port", type=int)
  parse_command_line()

  logger = logging.getLogger(__file__)
  logger.info("Starting Heron Shell")

  app.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()
