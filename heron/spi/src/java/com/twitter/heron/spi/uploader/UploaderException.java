// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.spi.uploader;

/**
 * Thrown to indicate that an error occurred while uploading package
 */
public class UploaderException extends RuntimeException {
  private static final long serialVersionUID = -254264900110286748L;

  public UploaderException(String message) {
    super(message);
  }

  public UploaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
