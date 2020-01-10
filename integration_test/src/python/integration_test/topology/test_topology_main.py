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

'''main method for integration test topology'''

import argparse
import logging
import sys

from integration_test.src.python.integration_test.topology.all_grouping.all_grouping \
    import all_grouping_builder

from integration_test.src.python.integration_test.topology.basic_one_task.basic_one_task \
  import basic_one_task_builder

from integration_test.src.python.integration_test.topology.bolt_double_emit_tuples\
    .bolt_double_emit_tuples import bolt_double_emit_tuples_builder

from integration_test.src.python.integration_test.topology.fields_grouping.fields_grouping \
  import fields_grouping_builder

from integration_test.src.python.integration_test.topology.global_grouping.global_grouping \
  import global_grouping_builder

from integration_test.src.python.integration_test.topology.multi_spouts_multi_tasks\
    .multi_spouts_multi_tasks import multi_spouts_multi_tasks_builder

from integration_test.src.python.integration_test.topology.none_grouping.none_grouping \
  import none_grouping_builder

from integration_test.src.python.integration_test.topology. one_bolt_multi_tasks\
    .one_bolt_multi_tasks import one_bolt_multi_tasks_builder

from integration_test.src.python.integration_test.topology.one_spout_bolt_multi_tasks\
    .one_spout_bolt_multi_tasks import one_spout_bolt_multi_tasks_builder

from integration_test.src.python.integration_test.topology.one_spout_multi_tasks\
    .one_spout_multi_tasks import one_spout_multi_tasks_builder

from integration_test.src.python.integration_test.topology.one_spout_two_bolts\
    .one_spout_two_bolts  import one_spout_two_bolts_builder

from integration_test.src.python.integration_test.topology.shuffle_grouping.shuffle_grouping \
  import shuffle_grouping_builder

from integration_test.src.python.integration_test.topology.streamlet.word_count_streamlet \
  import word_count_streamlet_builder

TOPOLOGY_BUILDERS = {
    'Heron_IntegrationTest_BasicOneTask': basic_one_task_builder,
    'Heron_IntegrationTest_AllGrouping': all_grouping_builder,
    'Heron_IntegrationTest_NoneGrouping': none_grouping_builder,
    'Heron_IntegrationTest_OneBoltMultiTasks': one_bolt_multi_tasks_builder,
    'Heron_IntegrationTest_OneSpoutBoltMultiTasks': one_spout_bolt_multi_tasks_builder,
    'Heron_IntegrationTest_ShuffleGrouping': shuffle_grouping_builder,
    'Heron_IntegrationTest_OneSpoutTwoBolts': one_spout_two_bolts_builder,
    'Heron_IntegrationTest_OneSpoutMultiTasks': one_spout_multi_tasks_builder,
    'Heron_IntegrationTest_MultiSpoutsMultiTasks': multi_spouts_multi_tasks_builder,
    'Heron_IntegrationTest_FieldsGrouping': fields_grouping_builder,
    'Heron_IntegrationTest_BoltDoubleEmitTuples': bolt_double_emit_tuples_builder,
    'Heron_IntegrationTest_GlobalGrouping': global_grouping_builder,
    "Heron_IntegrationTest_WordCountStreamlet": word_count_streamlet_builder
}

# pylint: disable=missing-docstring
def main():
  parser = argparse.ArgumentParser(description='Python topology submitter')
  parser.add_argument('-r', '--results-server-url', dest='results_url', required=True)
  parser.add_argument('-t', '--topology-name', dest='topology_name', required=True)
  args, unknown_args = parser.parse_known_args()
  if unknown_args:
    logging.error('Unknown argument passed to %s: %s', sys.argv[0], unknown_args[0])
    sys.exit(1)

  http_server_url = args.results_url

  # 1470884422_Heron_IntegrationTest_BasicOneTask_dca9bb1c-dd3b-4ea6-97dc-ea0cea265adc
  # --> Heron_IntegrationTest_BasicOneTask
  topology_name_with_uuid = args.topology_name
  topology_name = '_'.join(topology_name_with_uuid.split('_')[1:-1])

  if topology_name not in TOPOLOGY_BUILDERS:
    logging.error("%s not found in the list", topology_name)
    sys.exit(2)

  builder = TOPOLOGY_BUILDERS[topology_name]
  topo_class = builder(topology_name_with_uuid, http_server_url)
  topo_class.write()

if __name__ == '__main__':
  main()
