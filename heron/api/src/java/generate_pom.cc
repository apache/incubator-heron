/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#include <iostream>
#include <string>

#define HERON_VERSION "0.14.6"

int main(int argc, char** argv) {
  std::string tmpl(R"EOF(<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.twitter.heron</groupId>
  <artifactId>libtensorflow</artifactId>
  <version>{{HERON_VERSION}}</version>
  <packaging>jar</packaging>
  <name>heron-api</name>
  <url>http://www.heronstreaming.io</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:git@github.com:twitter/heron.git</connection>
    <developerConnection>scm:git:git@github.com:twitter/heron.git</developerConnection>
    <url>git@github.com:twitter/heron.git</url>
  </scm>
</project>
  )EOF");

  const std::string var("{{HERON_VERSION}}");
  const std::string val(HERON_VERSION);
  for (size_t pos = tmpl.find(var); pos != std::string::npos;
       pos = tmpl.find(var)) {
    tmpl.replace(pos, var.size(), val);
  }
  std::cout << tmpl;
  return 0;
}
