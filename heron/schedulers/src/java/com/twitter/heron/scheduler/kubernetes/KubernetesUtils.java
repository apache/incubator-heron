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
package com.twitter.heron.scheduler.kubernetes;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.squareup.okhttp.Response;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

import io.kubernetes.client.ApiException;

final class KubernetesUtils {

  private KubernetesUtils() {
  }

  static String getFetchCommand(Config config, Config runtime) {
    return String.format("%s %s .", Context.downloaderBinary(config),
        Runtime.topologyPackageUri(runtime).toString());
  }

  static void logResponseBodyIfPresent(Logger log, Response response) {
    try {
      log.log(Level.SEVERE, "Error details:\n" +  response.body().string());
    } catch (IOException ioe) {
      // ignore
    }
  }

  static void logExceptionWithDetails(Logger log, String message, Exception e) {
    log.log(Level.SEVERE, message + " " + e.getMessage());
    if (e instanceof ApiException) {
      log.log(Level.SEVERE, "Error details:\n" +  ((ApiException) e).getResponseBody());
    }
  }

  static String errorMessageFromResponse(Response response) {
    final String message = response.message();
    String details;
    try {
      details = response.body().string();
    } catch (IOException ioe) {
      // ignore
      details = ioe.getMessage();
    }
    return message + "\ndetails:\n" + details;
  }

  // https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/
  // #meaning-of-memory
  static String Megabytes(ByteAmount amount) {
    return String.format("%sMi", Long.toString(amount.asMegabytes()));
  }
}
