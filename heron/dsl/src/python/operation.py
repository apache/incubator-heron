# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''operation.py: module for defining the various different operations for python dsl'''

class OperationType(object):
  Input = 1
  Map = 2
  FlatMap = 3
  Filter = 4
  Sample = 5
  Join = 6
  Repartition = 7
  ReduceByKeyAndWindow = 8
  Output = 10

  AllOperations = [Input, Map, FlatMap, Filter, Sample, Join,
                   Repartition, ReduceByKeyAndWindow, Output]

  @staticmethod
  def valid(typ):
    return typ in OperationType.AllOperations
