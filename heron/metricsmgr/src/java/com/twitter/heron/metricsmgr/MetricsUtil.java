//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.metricsmgr;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;

public final class MetricsUtil {

  private static final String SOURCE_DELIMITER = "/";

  private static final String SOURCE_FORMAT = "%s:%d/%s/%s";

  static String createSource(String host, int port, String component, String instance) {
    return String.format(SOURCE_FORMAT, host, port, component, instance);
  }

  /**
   * The format of source is "host:port/componentName/instanceId"
   * So splitting the source would be an array with 3 elements:
   * ["host:port", componentName, instanceId]
   * @param record
   * @return
   */
  public static String[] splitRecordSource(MetricsRecord record) {
    return record.getSource().split(SOURCE_DELIMITER);
  }

  private MetricsUtil() {
  }
}
