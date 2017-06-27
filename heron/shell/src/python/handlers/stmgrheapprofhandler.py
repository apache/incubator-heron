# Copyright 2017 Twitter. All rights reserved.
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

''' stmgrheapprofhandler.py '''
import glob
import json
import os
import signal
import tornado.web

from heron.shell.src.python import utils

class StmgrHeapProfHandler(tornado.web.RequestHandler):
    """
    Responsible for getting the process ID for an instance.
    """

    # pylint: disable=attribute-defined-outside-init
    @tornado.web.asynchronous
    def get(self):
        ''' get method '''
        self.content_type = 'application/json'
        stmgr_pid_files = glob.glob('stmgr*.pid')
        try:
          pid_file = stmgr_pid_files[0]
          with open(pid_file, 'r') as f:
            pid = f.read()
            os.kill(int(pid), signal.SIGUSR1)
          self.write('Performing heap profiling on stream manager...')
          self.finish()
        except:
          self.write("Not stream manager found")
          self.set_status(404)
          self.finish()
