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
''' opts_unittest.py '''
import unittest2 as unittest
import mock
from mock import call, patch, Mock, MagicMock
import os
import subprocess
import sys
import tempfile
import heron.tools.cli.src.python.main as main
import heron.tools.cli.src.python.submit as submit
import heron.tools.cli.src.python.result as result
import heron.tools.common.src.python.utils.config as config

class OptsTest(unittest.TestCase):

  def setUp(self):
    tempfile.mkdtemp = MagicMock(return_value='/tmp/heron_tmp')
    main.check_environment = MagicMock(return_value=True)
    os.path.isdir = MagicMock(return_value=True)
    os.path.isfile = MagicMock(return_value=True)
    # Mock config calls
    config.parse_cluster_role_env = MagicMock(return_value=('local', 'user', 'default'))
    config.get_heron_release_file = MagicMock(return_value=None)
    config.create_tar = MagicMock(return_value=None)
    config.get_heron_lib_dir = MagicMock(return_value='/heron/lib/jars')
    config.get_heron_dir = MagicMock(return_value='/heron/home')
    config.get_java_path = MagicMock(return_value='/usr/lib/bin/java')
    config.get_heron_release_file = MagicMock(return_value='/heron/home/release.yaml')
    config.parse_override_config = MagicMock(return_value='/heron/home/override.yaml')
    # Mock result module
    result.render = MagicMock(return_value=None)
    result.is_successful = MagicMock(return_value=True)
    result.SimpleResult.__init__ = Mock(return_value=None)
    result.ProcessResult.__init__ = Mock(return_value=None)
    def launch(cl_args, topology_file, tmp_dir):
      return submit.launch_a_topology(cl_args, tmp_dir, topology_file, 'T.defn', 'WordCount')
    submit.launch_topologies = Mock(side_effect=launch)
    pass

  def test(self):
    subprocess.Popen = MagicMock()
    args = ["heron", "submit", "local", "~/.heron/examples/heron-examples.jar",
            "com.twitter.heron.examples.ExclamationTopology", "EX"]
    all_args_defn = ['/usr/lib/bin/java', '-client', '-Xmx1g', '-cp',
                '~/.heron/examples/heron-examples.jar:/heron/lib/jars/third_party/*',
                'com.twitter.heron.examples.ExclamationTopology', 'EX']
    all_args_submit = ['/usr/lib/bin/java', '-client', '-Xmx1g', '-cp',
                  ':/heron/lib/jars/scheduler/*:/heron/lib/jars/uploader/*:/heron/lib/jars/statemgr/*:/heron/lib/jars/packing/*',
                  'com.twitter.heron.scheduler.SubmitterMain', '--cluster', 'local', '--role',
                  'user', '--environment', 'default', '--heron_home', '/heron/home',
                  '--config_path', '/heron/home/conf/local', '--override_config_file',
                  '/heron/home/override.yaml', '--release_file', '/heron/home/release.yaml',
                  '--topology_package', '/tmp/heron_tmp/topology.tar.gz', '--topology_defn',
                  'T.defn', '--topology_bin', '~/.heron/examples/heron-examples.jar']
    env = {'HERON_OPTIONS':
           'cmdline.topologydefn.tmpdirectory=/tmp/heron_tmp,cmdline.topology.initial.state=RUNNING'}

    call_defn = call(all_args_defn, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1)
    call_submit = call(all_args_submit, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1)
    with patch.object(sys, 'argv', args):
      main.main()
      self.assertEqual(subprocess.Popen.call_args_list, [call_defn, call_submit])
