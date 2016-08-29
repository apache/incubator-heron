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

package org.apache.storm.generated;

public class ShellComponent {
  private String execution_command; // required
  private String script; // required

  public ShellComponent() {

  }

  public ShellComponent(String execution_command, String script) {
    this.execution_command = execution_command;
    this.script = script;
  }

  public String get_execution_command() {
    return execution_command;
  }

  public void set_execution_command(String execution_command) {
    this.execution_command = execution_command;
  }

  public String get_script() {
    return script;
  }

  public void set_script(String script) {
    this.script = script;
  }

}
