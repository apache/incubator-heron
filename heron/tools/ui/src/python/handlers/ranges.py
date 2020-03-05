#!/usr/bin/env python
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

''' ranges.py '''
import time

TIME_RANGES_3 = {
    "tenMinMetric": (0, 10 * 60, 'm'),
    "threeHourMetric": (0, 3 * 60 * 60, 'h'),
    "oneDayMetric": (0, 24 * 60 * 60, 'h'),
}

TIME_RANGES_6 = {
    "tenMinMetric": (0, 10 * 60, 'm'),
    "threeHourMetric": (0, 3 * 60 * 60, 'h'),
    "oneDayMetric": (0, 24 * 60 * 60, 'h'),

    "prevTenMinMetric": (10 * 60, 20 * 60, 'm'),
    "prevThreeHourMetric": (3 * 60 * 60, 6 * 60 * 60, 'h'),
    "prevOneDayMetric": (24 * 60 * 60, 48 * 60 * 60, 'h')
}


def get_time_ranges(ranges):
  '''
  :param ranges:
  :return:
  '''
  # get the current time
  now = int(time.time())

  # form the new
  time_slots = dict()

  for key, value in list(ranges.items()):
    time_slots[key] = (now - value[0], now - value[1], value[2])

  return (now, time_slots)
