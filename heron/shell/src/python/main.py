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

#!/usr/bin/env python2.7
''' main.py '''
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

# pylint: disable=unnecessary-lambda
def chain(cmd_list):
  """
  Feed output of one command to the next and return final output
  Returns string output of chained application of commands.
  """
  return {
      'command' : ' | '.join(map(lambda x: ' '.join(x), cmd_list)),
      'stdout' : stream_to_string(reduce(pipe, [None] + cmd_list))
  }

def get_container_id(instance_id):
  ''' get container id '''
  return instance_id.split('_')[1]  # Format: container_<index>_component_name_<index>

def get_asset(asset_name):
  ''' get assset '''
  return pkgutil.get_data("heron.shell", os.path.join("assets", asset_name))


class PidHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the process ID for an instance.
  """

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, instance_id):
    ''' get method '''
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

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, pid):
    ''' get method '''
    body = str_cmd(['jmap', '-histo', pid])
    self.content_type = 'application/json'
    self.write(json.dumps(body))
    self.finish()

class JmapHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the jmap for a jvm process given its pid.
  """

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, pid):
    ''' get method '''
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

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, pid):
    ''' get method '''
    body = str_cmd(['jstack', pid])
    self.content_type = 'application/json'
    self.write(json.dumps(body))
    self.finish()

class BrowseHandler(tornado.web.RequestHandler):
  """
  Responsible for browsing directories.
  """

  # pylint: disable=attribute-defined-outside-init
  @tornado.web.asynchronous
  def get(self, path):
    ''' get method '''
    if not path:
      path = "."
    if path.startswith("/"):
      self.write("Only relative paths are allowed")
      self.set_status(403)
      self.finish()
      return
    t = Template(get_asset("browse.html"))
    args = dict(
        path=path,
        listing=utils.get_listing(path),
        format_prefix=utils.format_prefix,
        stat=stat,
        get_stat=utils.get_stat,
        os=os,
        css=get_asset("bootstrap.css")
    )
    self.write(t.generate(**args))
    self.finish()

class FileStatsHandler(tornado.web.RequestHandler):
  """
  Get the file stats in JSON format given the path.
  """
  @tornado.web.asynchronous
  def get(self, path):
    ''' get method '''
    path = tornado.escape.url_unescape(path)
    if not path:
      path = "."

    # User should not be able to access anything outside
    # of the dir that heron-shell is running in. This ensures
    # sandboxing. So we don't allow absolute paths and parent
    # accessing.
    if path.startswith("/") or ".." in path:
      self.write("Only relative paths inside job dir are allowed")
      self.set_status(403)
      self.finish()
      return
    listing = utils.get_listing(path)
    file_stats = {}
    for fn in listing:
      try:
        is_dir = False
        formatted_stat = utils.format_prefix(fn, utils.get_stat(path, fn))
        if stat.S_ISDIR(utils.get_stat(path, fn).st_mode):
          is_dir = True
        file_stats[fn] = {
            "formatted_stat": formatted_stat,
            "is_dir": is_dir,
            "path": tornado.escape.url_escape(os.path.join(path, fn)),
        }
        if fn == "..":
          path_fragments = path.split("/")
          if not path_fragments:
            file_stats[fn]["path"] = "."
          else:
            file_stats[fn]["path"] = tornado.escape.url_escape("/".join(path_fragments[:-1]))
      except:
        continue
    self.write(json.dumps(file_stats))
    self.finish()

class FileHandler(tornado.web.RequestHandler):
  """
  Responsible for creating the web page for files. The html
  will in turn call the /filedata/ endpoint to get the file data.
  """
  @tornado.web.asynchronous
  def get(self, path):
    """ get method """
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
        filename=path,
        jquery=get_asset("jquery.js"),
        pailer=get_asset("jquery.pailer.js"),
        css=get_asset("bootstrap.css"),
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
    """ get method """
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
    """ set headers """
    self.set_header("Content-Disposition", "attachment")

app = tornado.web.Application([
    (r"^/jmap/([0-9]+$)", JmapHandler),
    (r"^/histo/([0-9]+$)", MemoryHistogramHandler),
    (r"^/jstack/([0-9]+$)", JstackHandler),
    (r"^/pid/(.*)", PidHandler),
    (r"^/browse/(.*)", BrowseHandler),
    (r"^/file/(.*)", FileHandler),
    (r"^/filedata/(.*)", FileDataHandler),
    (r"^/filestats/(.*)", FileStatsHandler),
    (r"^/download/(.*)", DownloadHandler, {"path":"."}),
])


if __name__ == '__main__':
  define("port", default=9999, help="Runs on the given port", type=int)
  parse_command_line()

  logger = logging.getLogger(__file__)
  logger.info("Starting Heron Shell")

  app.listen(options.port)
  tornado.ioloop.IOLoop.instance().start()
