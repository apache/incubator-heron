//  Copyright 2018 Twitter. All rights reserved.
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

package com.twitter.heron.api.exception;

/**
 * Thrown to indicate that the application has attempted to submit an invalid topology.
 */
public class TopologySubmissionException extends RuntimeException {

  private static final long serialVersionUID = -5045350685867299824L;

  public TopologySubmissionException(String message) {
    super(message);
  }

  public TopologySubmissionException(String message, Throwable cause) {
    super(message, cause);
  }
}
