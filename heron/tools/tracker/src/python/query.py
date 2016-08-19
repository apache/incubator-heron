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
""" query.py """
import tornado.httpclient
import tornado.gen

from heron.tools.tracker.src.python.query_operators import *


####################################################################
# Parsing and executing the query string.
####################################################################

# pylint: disable=no-self-use
class Query(object):
  """Execute the query for metrics. Uses Tracker to get
     individual metrics that are part of the query.
     Example usage:
        query = Query(tracker)
        result = query.execute(tmaster, query_string)"""
  # pylint: disable=undefined-variable
  def __init__(self, tracker):
    self.tracker = tracker
    self.operators = {
        'TS':TS,
        'DEFAULT':Default,
        'MAX':Max,
        'SUM':Sum,
        'SUBTRACT':Subtract,
        'PERCENTILE':Percentile,
        'DIVIDE':Divide,
        'MULTIPLY':Multiply,
        'RATE':Rate
    }


  # pylint: disable=attribute-defined-outside-init, no-member
  @tornado.gen.coroutine
  def execute_query(self, tmaster, query_string, start, end):
    """ execute query """
    if not tmaster:
      raise Exception("No tmaster found")
    self.tmaster = tmaster
    root = self.parse_query_string(query_string)
    metrics = yield root.execute(self.tracker, self.tmaster, start, end)
    raise tornado.gen.Return(metrics)

  def find_closing_braces(self, query):
    """Find the index of the closing braces for the opening braces
    at the start of the query string. Note that first character
    of input string must be an opening braces."""
    if query[0] != '(':
      raise Exception("Trying to find closing braces for no opening braces")
    num_open_braces = 0
    for i in range(len(query)):
      c = query[i]
      if c == '(':
        num_open_braces += 1
      elif c == ')':
        num_open_braces -= 1
      if num_open_braces == 0:
        return i
    raise Exception("No closing braces found")

  def get_sub_parts(self, query):
    """The subparts are seperated by a comma. Make sure
    that commas inside the part themselves are not considered."""
    parts = []
    num_open_braces = 0
    delimiter = ','
    last_starting_index = 0
    for i in range(len(query)):
      if query[i] == '(':
        num_open_braces += 1
      elif query[i] == ')':
        num_open_braces -= 1
      elif query[i] == delimiter and num_open_braces == 0:
        parts.append(query[last_starting_index: i].strip())
        last_starting_index = i + 1
    parts.append(query[last_starting_index:].strip())
    return parts

  def parse_query_string(self, query):
    """Returns a parse tree for the query, each of the node is a
    subclass of Operator. This is both a lexical as well as syntax analyzer step."""
    if not query:
      return None
    # Just braces do not matter
    if query[0] == '(':
      index = self.find_closing_braces(query)
      # This must be the last index, since this was an NOP starting brace
      if index != len(query) - 1:
        raise Exception("Invalid syntax")
      else:
        return self.parse_query_string(query[1:-1])
    start_index = query.find("(")
    # There must be a ( in the query
    if start_index < 0:
      # Otherwise it must be a constant
      try:
        constant = float(query)
        return constant
      except ValueError:
        raise Exception("Invalid syntax")
    token = query[:start_index]
    if token not in self.operators:
      raise Exception("Invalid token: " + token)

    # Get sub components
    rest_of_the_query = query[start_index:]
    braces_end_index = self.find_closing_braces(rest_of_the_query)
    if braces_end_index != len(rest_of_the_query) - 1:
      raise Exception("Invalid syntax")
    parts = self.get_sub_parts(rest_of_the_query[1:-1])

    # parts are simple strings in this case
    if token == "TS":
      # This will raise exception if parts are not syntactically correct
      return self.operators[token](parts)

    children = []
    for part in parts:
      children.append(self.parse_query_string(part))

    # Make a node for the current token
    node = self.operators[token](children)
    return node
