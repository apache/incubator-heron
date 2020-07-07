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

'''constants.py: constants for integration test for heron'''
INTEGRATION_TEST_MOCK_MESSAGE_ID = "__integration_test_mock_message_id"
INTEGRATION_TEST_TERMINAL = "__integration_test_mock_terminal"
INTEGRATION_TEST_CONTROL_STREAM_ID = "__integration_test_control_stream_id"

# internal config key
MAX_EXECUTIONS = 10
HTTP_POST_URL_KEY = "http.post.url"

# user defined config key
USER_SPOUT_CLASSPATH = "user.spout.classpath"
USER_BOLT_CLASSPATH = "user.bolt.classpath"
# user defined max executions
USER_MAX_EXECUTIONS = "user.max.exec"
