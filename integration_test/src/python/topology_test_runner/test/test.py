''' main '''

# to run this file:
# bazel build --config=darwin integration_test/src/...
# ./bazel-bin/integration_test/src/python/topology_test_runner/test/topology-test-runner-test.pex

from heron.proto import physical_plan_pb2
import json

def main():
  ''' main '''
  '''local_pplan_path = "/Users/yaoli/.herondata/repository/state/local/pplans/" \
                + '20180608142329_IntegrationTest_NonGrouping_df5d5551-c237-44dc-8951-b479dc20c082'
  with open(local_pplan_path, "r") as pplan_file:
    pplan_string = pplan_file.read().rstrip()
  print pplan_string
  pplan = physical_plan_pb2.PhysicalPlan()
  pplan.ParseFromString(pplan_string)
  #print pplan'''

  file_path = "/Users/yaoli/workspace/incubator-heron/integration_test/src/java/org/apache/heron/integration_topology_test/topology/basic_topology_one_task/BasicTopologyOneTaskResults.json"
  with open(file_path, "r") as expected_result_file:
    expected_result = expected_result_file.read().rstrip()

  decoder = json.JSONDecoder(strict=False)
  expected_results = decoder.decode(expected_result)
  print expected_results["topology"]["bolts"]

if __name__ == '__main__':
    main()
