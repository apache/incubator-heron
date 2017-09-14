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
''' examples.py '''
import os
import re
import zipfile

from heron.common.src.python.utils.log import Log
from heron.tools.cli.src.python.result import SimpleResult, Status
import heron.tools.cli.src.python.args as cli_args
import heron.tools.cli.src.python.submit as submit
import heron.tools.common.src.python.utils.config as config


heron_java_examples = dict(
    AckingTopology="acking",
    ComponentJVMOptionsTopology="component-jvm-options",
    CustomGroupingTopology="custom-grouping",
    ExclamationTopology="exclamation",
    MultiSpoutExclamationTopology="ms-exclamation",
    MultiStageAckingTopology="ms-acking",
    SentenceWordCountTopology="sentence-wordcount",
    SlidingWindowTopology="sliding-window",
    TaskHookTopology="taskhook",
    WordCountTopology="wordcount")


base_examples_package = "com.twitter.heron.examples."

heron_examples = None

def examples():
  '''
  :return:
  '''
  global heron_examples
  if heron_examples is None:
    found_examples = installed_examples()

    examples_list = []
    for name, eid in heron_java_examples.items():
      clazz = base_examples_package + name
      if clazz in found_examples:
        examples_list.append(dict(
            name=name,
            id=eid,
            clazz=clazz))

    heron_examples = sorted(examples_list, key=lambda ex: ex["name"])

  return heron_examples


def installed_examples():
  heron_examples_jar = config.get_heron_examples_jar()
  archive = zipfile.ZipFile(heron_examples_jar, 'r')

  def to_clazz(c):
    return c.replace("/", ".").replace(".class", "")

  example_re = '^com/twitter/heron/examples/[a-zA-Z0-9.-]+.class'
  pattern = re.compile(example_re)
  return set((to_clazz(c) for c in archive.namelist() if pattern.match(c)))

def classname(example_id):
  for example in examples():
    if example["id"] == example_id:
      return example["clazz"]
  return None

def example_id_error(example_id):
  args = (example_id, examples_string())
  return "Example id '%s' does not exist.\nAvailable examples:\n%s" % args

def no_examples_error():
  return "Could not find examples at '%s'" % config.get_heron_examples_jar()

def has_examples():
  examples_jar = config.get_heron_examples_jar()

  # if the file does not exist and is not a file
  if not os.path.isfile(examples_jar):
    Log.warn("Required file not found: %s" % examples_jar)
    return False

  return True

def examples_string():
  lines = []
  col_width = max(len(ex["name"]) for ex in examples()) + 4
  lines.append("".join(word.ljust(col_width) for word in ("Name", "ID")))
  for ex in examples():
    lines.append("".join(word.ljust(col_width) for word in (ex["name"], ex["id"])))

  return "\n".join(lines)


def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'examples',
      help='{list|run} heron examples',
      usage="%(prog)s <command>",
      formatter_class=config.SubcommandHelpFormatter,
      add_help=True)

  ex_subparsers = parser.add_subparsers(
      title="Commands",
      description=None)


  list_parser = ex_subparsers.add_parser(
      'list',
      help='Print packaged heron examples',
      usage="heron examples list",
      add_help=True)
  list_parser.set_defaults(subcommand='examples-list')


  run_parser = ex_subparsers.add_parser(
      'run',
      help='Run a heron example',
      usage="heron examples run cluster example-id",
      add_help=True)
  run_parser.set_defaults(subcommand='examples-run')

  run_parser.add_argument(
      'cluster',
      help='Cluster to run topology')

  run_parser.add_argument(
      'example-id',
      help='Example id to run')

  # add optional run arguments
  cli_args.add_config(run_parser)
  cli_args.add_service_url(run_parser)
  cli_args.add_verbose(run_parser)

  return parser

# pylint: disable=unused-argument
def list_examples(command, parser, cl_args, unknown_args):
  if has_examples():
    print examples_string()
  else:
    print no_examples_error()
  return SimpleResult(Status.Ok)

# pylint: disable=unused-argument
def run_example(command, parser, cl_args, unknown_args):
  topology_file = config.get_heron_examples_jar() if has_examples() else None
  if topology_file is None:
    return SimpleResult(Status.InvocationError, no_examples_error())

  example_id = cl_args['example-id']
  topology_classname = classname(example_id)
  if topology_classname is None:
    return SimpleResult(Status.InvocationError, example_id_error(example_id))

  cl_args['topology-file-name'] = topology_file
  cl_args['topology-class-name'] = topology_classname

  return submit.run("submit", parser, cl_args, [example_id])


# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  '''
  :param command:
  :param parser:
  :param cl_args:
  :param unknown_args:
  :return:
  '''
  if command == 'examples-run':
    return run_example(command, parser, cl_args, unknown_args)
  else:
    return list_examples(command, parser, cl_args, unknown_args)
