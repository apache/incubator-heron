#!/usr/bin/env python2.7

import os, sys
import subprocess
#print ("./heron-executor <shardid> <topname> <topid> <topdefnfile> "
#       " <instance_distribution> <zknode> <zkroot> <tmaster_binary> <stmgr_binary> "
#       " <metricsmgr_classpath> <instance_jvm_opts_in_base64> <classpath> "
#       " <master_port> <tmaster_controller_port> <tmaster_stats_port> <heron_internals_config_file> "
#       " <component_rammap> <component_jvm_opts_in_base64> <pkg_type> <topology_jar_file>"
#       " <heron_java_home> <shell-port> <heron_shell_binary> <metricsmgr_port>"
#       " <cluster> <role> <environ> <instance_classpath> <metrics_sinks_config_file> "
#       " <scheduler_classpath> <scheduler_port>")


#print ("\n" + "To execute a python instance:\n" +
#       "  - <classpath>: the path to the Python Heron Instance executable\n" +
#       "  - <pkg_type>: pex\n" +
#       "  - <topology_jar_file>: the path to the topology pex file\n" +
#       "<instance_jvm_opts>, <component_rammap>, <component_jvm_opts>, <instance_classpath> are ignored")

sharids = ['0', '1']

pwd = "/Users/tnojima/.herondata/topologies/local/tnojima/WordCountTopology"
workspace = "/Users/tnojima/workspace/heron"

heron_executor = os.path.join(workspace, "bazel-bin/heron/executor/src/python/heron-executor")

topology_name = "WordCountTopology"
topology_id = "WordCountTopology4fc3cbf0-1c3e-4fdc-83f4-9079e4c6e5f9"
defn_file = os.path.join(pwd, "WordCountTopology.defn")
instance_dist = "1:word_spout:2:0:count_bolt:1:0"
zk_root = "/Users/tnojima/.herondata/repository/state/local"

tmaster_bin = os.path.join(pwd, "heron-core/bin/heron-tmaster")
stmgr_bin = os.path.join(pwd, "heron-core/bin/heron-stmgr")
shell_bin = os.path.join(pwd, "heron-core/bin/heron-shell")
metrics_mgr_clspath = pwd + "/" + "heron-core/lib/metricsmgr/*"

py_heron_instance = os.path.join(workspace, "bazel-bin/heron/instance/src/python/single_thread_heron_instance")
heron_internal_config = os.path.join(pwd, "heron-conf/heron_internals.yaml")
metrics_sinks_config = os.path.join(pwd, "heron-conf/metrics_sinks.yaml")
rammap_sample = "word_spout:536870912,count_bolt:536870912"
scheduler_classpath = "./heron-core/lib/scheduler/*:./heron-core/lib/packing/*:./heron-core/lib/statemgr/*"

pexfile_path = os.path.join(workspace, "bazel-bin/heron/examples/src/python/word_count.pex")

if len(sys.argv) != 2:
  print "Not enough arg"
  sys.exit(1)
elif sys.argv[1] == '0':
  print "Starting tmaster -- container 0"
  sharid = '0'
  open_ports = [str(i) for i in range(12340, 12380)]
else:
  print "Starting instance"
  sharid = '1'
  open_ports = [str(i) for i in range(22340, 22380)]

cmd_arg = [
  heron_executor,
  sharid,
  topology_name,
  topology_id,
  defn_file,
  instance_dist,
  "LOCALMODE",
  zk_root,
  tmaster_bin,
  stmgr_bin,
  metrics_mgr_clspath,
  "",   # instance jvm opts
  py_heron_instance, # classpath
  open_ports.pop(), # master port
  open_ports.pop(), # tmaster controller port
  open_ports.pop(), # tmaster stats port
  heron_internal_config,
  rammap_sample,
  "", # component jvm opts
  "pex",
  pexfile_path, # topology_jar_file
  "/Library/Java/JavaVirtualMachines/jdk1.8.0_91.jdk/Contents/Home",
  open_ports.pop(), # shell port
  shell_bin,
  open_ports.pop(), # metrics mgr port
  "local",
  "tnojima",
  "default",
  "", # instance class path ignored
  metrics_sinks_config,
  scheduler_classpath,
  open_ports.pop()  # scheduler_port
]

os.chdir(pwd)
print "Running heron-executor"
subprocess.Popen(cmd_arg).communicate()



